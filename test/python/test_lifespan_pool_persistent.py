"""Runner protocol regressions; fake transport does not prove native compatibility.

Run with: uv run --python 3.14 --no-project python -m unittest discover \
    -s test/python -p test_lifespan_pool_persistent.py -v
"""

import asyncio
import importlib.util
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import patch


class ChannelClosed(Exception):
    pass


class Control:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.native_calls = []

    async def async_receive(self, *, native_only=False):
        self.native_calls.append(native_only)
        message = await self.queue.get()
        if isinstance(message, Exception):
            raise message
        return message

    def send(self, message):
        self.queue.put_nowait(message)


class ErlangStub(ModuleType):
    ChannelClosed = ChannelClosed

    def __init__(self):
        super().__init__('erlang')
        self.outcomes = asyncio.Queue()

    def Channel(self, ref):
        return ref

    def send(self, pid, message):
        self.outcomes.put_nowait((pid, message))


class PersistentLifespanTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bridge = ErlangStub()
        path = Path(__file__).resolve().parents[2] / 'priv/hornbeam_lifespan_runner.py'
        spec = importlib.util.spec_from_file_location('lifespan_pool_under_test', path)
        self.runner = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'erlang': self.bridge}):
            spec.loader.exec_module(self.runner)
        self.controls = []
        self.tasks = []
        self.patches = []

    async def asyncTearDown(self):
        for control in self.controls:
            control.send(ChannelClosed())
        for task in self.tasks:
            if not task.done():
                task.cancel()
        for task in self.tasks:
            try:
                await task
            except (asyncio.CancelledError, RuntimeError):
                pass
        for p in self.patches:
            p.stop()
        self.assertFalse(self.runner._unfinished_tasks)

    def launch(self, app, mount=None, timeout_ms=200):
        p = patch.object(self.runner, 'load_app', return_value=app)
        p.start()
        self.patches.append(p)
        channel = Control()
        token = len(self.tasks) + 1
        task = asyncio.create_task(self.runner.run_lifespan(
            'manager', token, channel, 'test_app', 'app', mount, timeout_ms))
        self.controls.append(channel)
        self.tasks.append(task)
        channel.send('lifespan.startup')
        return task, channel

    async def outcome(self, kind):
        pid, (tag, token, response) = await asyncio.wait_for(self.bridge.outcomes.get(), 1)
        self.assertEqual(pid, 'manager')
        self.assertEqual(tag, 'lifespan')
        self.assertGreater(token, 0)
        self.assertEqual(response['type'], kind)
        return response

    async def test_background_worker_runs_between_startup_and_shutdown(self):
        ticks = asyncio.Queue()
        cleaned = asyncio.Event()
        app_loop = None

        async def app(scope, receive, send):
            nonlocal app_loop
            app_loop = asyncio.get_running_loop()
            self.assertEqual((await receive())['type'], 'lifespan.startup')
            scope['state']['ready'] = True

            async def worker():
                while True:
                    ticks.put_nowait('tick')
                    await asyncio.sleep(0.001)

            worker_task = asyncio.create_task(worker())
            try:
                await send({'type': 'lifespan.startup.complete'})
                self.assertEqual((await receive())['type'], 'lifespan.shutdown')
                await send({'type': 'lifespan.shutdown.complete'})
                # Report must wait for post-send app cleanup.
                await asyncio.sleep(0.01)
            finally:
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
                cleaned.set()

        task, channel = self.launch(app)
        result = await self.outcome('lifespan.startup.complete')
        self.assertEqual(result['state'], {'ready': True})
        self.assertIs(self.runner.get_state(), result['state'])
        await asyncio.wait_for(ticks.get(), 1)
        await asyncio.wait_for(ticks.get(), 1)
        self.assertFalse(task.done())
        self.assertIs(app_loop, asyncio.get_running_loop())
        channel.send('lifespan.shutdown')
        await self.outcome('lifespan.shutdown.complete')
        self.assertTrue(cleaned.is_set())
        await task
        self.assertEqual(channel.native_calls, [True, True])

    async def test_explicit_startup_failure_not_lost_on_immediate_exit(self):
        async def app(scope, receive, send):
            await receive()
            await send({'type': 'lifespan.startup.failed', 'message': 'broken'})
        task, _ = self.launch(app)
        response = await self.outcome('lifespan.startup.failed')
        self.assertEqual(response['message'], 'broken')
        await task

    async def test_startup_exception_is_failure_not_unsupported(self):
        async def app(scope, receive, send):
            raise ValueError('exploded')
        task, _ = self.launch(app)
        response = await self.outcome('lifespan.startup.failed')
        self.assertEqual(response['message'], 'exploded')
        await task

    async def test_silent_return_is_unsupported(self):
        async def app(scope, receive, send):
            return
        task, _ = self.launch(app)
        await self.outcome('lifespan.not_supported')
        await task

    async def test_startup_timeout_cancels_application(self):
        cleaned = asyncio.Event()
        async def app(scope, receive, send):
            try:
                await asyncio.Future()
            finally:
                cleaned.set()
        task, _ = self.launch(app, timeout_ms=10)
        response = await self.outcome('lifespan.startup.failed')
        self.assertEqual(response['message'], 'Startup timeout')
        await asyncio.wait_for(task, 1)
        self.assertTrue(cleaned.is_set())

    async def test_shutdown_timeout_is_bounded_and_cancels_app(self):
        cleaned = asyncio.Event()
        async def app(scope, receive, send):
            try:
                await receive()
                await send({'type': 'lifespan.startup.complete'})
                await receive()
                await asyncio.Future()
            finally:
                cleaned.set()
        task, channel = self.launch(app, timeout_ms=10)
        await self.outcome('lifespan.startup.complete')
        channel.send('lifespan.shutdown')
        await self.outcome('lifespan.shutdown.failed')
        await asyncio.wait_for(task, 1)
        self.assertTrue(cleaned.is_set())

    async def test_shutdown_completion_does_not_hide_hung_cleanup(self):
        cleaned = asyncio.Event()
        async def app(scope, receive, send):
            try:
                await receive()
                await send({'type': 'lifespan.startup.complete'})
                await receive()
                await send({'type': 'lifespan.shutdown.complete'})
                await asyncio.Future()
            finally:
                cleaned.set()
        task, channel = self.launch(app, timeout_ms=10)
        await self.outcome('lifespan.startup.complete')
        channel.send('lifespan.shutdown')
        response = await self.outcome('lifespan.shutdown.failed')
        self.assertEqual(response['message'], 'Shutdown task timeout')
        await asyncio.wait_for(task, 1)
        self.assertTrue(cleaned.is_set())

    async def test_cancellation_resistant_application_cannot_block_runner(self):
        release = asyncio.Event()
        async def app(scope, receive, send):
            await receive()
            await send({'type': 'lifespan.startup.complete'})
            await receive()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                await release.wait()
        task, channel = self.launch(app, timeout_ms=10)
        await self.outcome('lifespan.startup.complete')
        with self.assertLogs(self.runner.__name__, level='ERROR'):
            channel.send('lifespan.shutdown')
            await self.outcome('lifespan.shutdown.failed')
            await asyncio.wait_for(task, 1)
        pending = set(self.runner._unfinished_tasks)
        self.assertEqual(len(pending), 1)
        release.set()
        await asyncio.wait(pending, timeout=1)
        await asyncio.sleep(0)
        self.assertFalse(self.runner._unfinished_tasks)

    async def test_shutdown_failure_propagates(self):
        async def app(scope, receive, send):
            await receive()
            await send({'type': 'lifespan.startup.complete'})
            await receive()
            await send({'type': 'lifespan.shutdown.failed', 'message': 'cleanup failed'})
        task, channel = self.launch(app)
        await self.outcome('lifespan.startup.complete')
        channel.send('lifespan.shutdown')
        response = await self.outcome('lifespan.shutdown.failed')
        self.assertEqual(response['message'], 'cleanup failed')
        await task

    async def test_channel_close_cancels_waiting_application(self):
        cleaned = asyncio.Event()
        async def app(scope, receive, send):
            try:
                await receive()
                await send({'type': 'lifespan.startup.complete'})
                await receive()
            finally:
                cleaned.set()
        task, channel = self.launch(app)
        await self.outcome('lifespan.startup.complete')
        channel.send(ChannelClosed())
        await asyncio.wait_for(task, 1)
        self.assertTrue(cleaned.is_set())

    async def test_control_receive_failure_still_cleans_app_and_mount(self):
        cleaned = asyncio.Event()
        app_tasks = []
        failure = RuntimeError('injected control receive failure')

        async def app(scope, receive, send):
            app_tasks.append(asyncio.current_task())
            try:
                await receive()
                scope['state']['ready'] = True
                await send({'type': 'lifespan.startup.complete'})
                await receive()
            finally:
                cleaned.set()

        task, channel = self.launch(app, mount='control_failure')
        await self.outcome('lifespan.startup.complete')
        self.assertIn('control_failure', self.runner._lifespan_states)
        channel.send(failure)
        try:
            with self.assertRaises(RuntimeError) as raised:
                await asyncio.wait_for(task, 1)
            self.assertIs(raised.exception, failure)
            self.assertTrue(cleaned.is_set())
            self.assertTrue(app_tasks[0].done())
            self.assertNotIn('control_failure', self.runner._lifespan_states)
            self.assertFalse(self.runner._unfinished_tasks)
        finally:
            # Also clean up when demonstrating the regression against old code.
            if not app_tasks[0].done():
                app_tasks[0].cancel()
                try:
                    await app_tasks[0]
                except asyncio.CancelledError:
                    pass

    async def test_immediate_exception_after_startup_complete_reports_failure(self):
        async def app(scope, receive, send):
            await receive()
            await send({'type': 'lifespan.startup.complete'})
            raise RuntimeError('immediate app failure')

        task, _ = self.launch(app)
        response = await self.outcome('lifespan.startup.failed')
        self.assertEqual(response['message'], 'immediate app failure')
        await task
        self.assertTrue(self.bridge.outcomes.empty())

    async def test_immediate_return_after_startup_complete_reports_failure(self):
        async def app(scope, receive, send):
            await receive()
            await send({'type': 'lifespan.startup.complete'})

        task, _ = self.launch(app)
        response = await self.outcome('lifespan.startup.failed')
        self.assertEqual(response['message'], 'Application exited after startup response')
        await task
        self.assertTrue(self.bridge.outcomes.empty())

    async def test_unexpected_exit_after_startup_reaches_submitter(self):
        exit_app = asyncio.Event()

        async def app(scope, receive, send):
            await receive()
            await send({'type': 'lifespan.startup.complete'})
            await exit_app.wait()

        task, _ = self.launch(app)
        await self.outcome('lifespan.startup.complete')
        exit_app.set()
        with self.assertRaisesRegex(RuntimeError, 'exited before shutdown'):
            await task

    async def test_mount_state_is_independent_and_removed_at_shutdown(self):
        async def app(scope, receive, send):
            await receive()
            scope['state']['name'] = 'mount'
            await send({'type': 'lifespan.startup.complete'})
            await receive()
            await send({'type': 'lifespan.shutdown.complete'})
        first, first_channel = self.launch(app, mount='first')
        await self.outcome('lifespan.startup.complete')
        second, second_channel = self.launch(app, mount='second')
        await self.outcome('lifespan.startup.complete')
        self.assertIsNot(self.runner.get_state('first'), self.runner.get_state('second'))
        first_channel.send('lifespan.shutdown')
        await self.outcome('lifespan.shutdown.complete')
        await first
        self.assertEqual(self.runner.get_state('first'), {})
        self.assertEqual(self.runner.get_state('second'), {'name': 'mount'})
        second_channel.send('lifespan.shutdown')
        await self.outcome('lifespan.shutdown.complete')
        await second


if __name__ == '__main__':
    unittest.main()
