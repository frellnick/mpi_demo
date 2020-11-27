# exceptions.py

class Bypass(Exception):
    def __init__(self):
        self.message = 'Bypass detected.  Skipping remaining linkage steps.'
        super().__init__(self.message)