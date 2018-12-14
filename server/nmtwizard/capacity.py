class Capacity(list):
    # utility class to make operations on gpu/cpu capacities
    # inherits from list to keep json serialization
    def __init__(self, ngpus=0, ncpus=0):
        list.__init__(self, (int(ngpus), int(ncpus)))

    def incr_ngpus(self, v=1):
        self[0] += v

    def incr_ncpus(self, v=1):
        self[1] += v

    @property
    def ngpus(self):
        return self[0]

    @property
    def ncpus(self):
        return self[1]

    def __add__(self, other):
        return Capacity(self.ngpus + other.ngpus, self.ncpus + other.ncpus)

    def __iadd__(self, other):
        self[0] += other.ngpus
        self[1] += other.ncpus
        return self

    def __sub__(self, other):
        return Capacity(self.ngpus - other.ngpus, self.ncpus - other.ncpus)

    def __isub__(self, other):
        self[0] -= other.ngpus
        self[1] -= other.ncpus
        return self

    def __lt__(self, other):
        return (self.ngpus == other.ngpus and self.ncpus < other.ncpus) \
               or self.ngpus < other.ngpus

    def __le__(self, other):
        return self == other or self < other

    def __eq__(self, other):
        return self.ngpus == other.ngpus and self.ncpus == other.ncpus
