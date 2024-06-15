import os

class AtomicWriter:
    def __init__(self, path):
        self.path = path
        self.tmp_path = path + '.tmp'

    def write(self, data):
        with open(self.tmp_path, 'w+') as f:
            f.write(data)
            #f.flush()
            os.fsync(f.fileno())
        os.rename(self.tmp_path, self.path)