
class SafeTx:
    def __init__(self, to: str, value: int, data: str, contract_method=None, contract_inputs_values=None):
        self._to = to
        self._value = str(value)
        self._data = data
        self._contract_method = contract_method
        self._contract_inputs_values = contract_inputs_values

    def __iter__(self):
        return iter([self._to, self._value, self._data, self._contract_method, self._contract_inputs_values])

    @property
    def to(self):
        return self._to

    @property
    def value(self):
        return self._value

    @property
    def data(self):
        return self._data

    @property
    def contract_method(self):
        return self._contract_method

    @property
    def contract_inputs_values(self):
        return self._contract_inputs_values