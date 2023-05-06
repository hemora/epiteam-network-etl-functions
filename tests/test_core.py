from typing import Any
from src.core.core_abstract import AbstractHandler

class A(AbstractHandler):
    def foo_a(self, x: str) -> str:
        return f"A stage processing {x}"

    def handle(self, request: Any) -> Any:
        return super().handle(self.foo_a(request))

class B(AbstractHandler):
    def foo_b(self, x: str) -> int:
        return 4

    def handle(self, request: Any) -> Any:
        return super().handle(self.foo_b(request))

def test_abstract_handle_method():
    """
    """
    t_A = A()

    result = t_A.handle("perro")

    assert (type(result) == str) and (result == "A stage processing perro")

def test_abstract_handle_when_next():
    """
    """
    chain = A().set_next(B())

    result = chain.handle("perro")

    assert (type(result) == int) and (result == 4)

