# fake typemap
class FakeTypeMap(dict):
    # override __init__

    def __init__(self, configClass):
        # configuration class
        self.configClass = configClass

    # override __getitem__
    def __getitem__(self, k):
        return self.setdefault(k, self.configClass)
