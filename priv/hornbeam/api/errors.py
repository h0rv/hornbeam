class RuntimeUnavailable(RuntimeError):
    pass


class RuntimeProtocolError(RuntimeError):
    pass


class SubscriptionClosed(RuntimeError):
    pass


class SubscriptionOverflow(RuntimeError):
    pass
