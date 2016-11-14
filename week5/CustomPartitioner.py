
from mrjob.job import MRJob
from sys import stderr
import numpy as np
from operator import itemgetter
from random import random


class CustomPartitioner(MRJob):
    def __init__(self, *args, **kwargs):
        super(CustomPartitioner, self).__init__(*args, **kwargs)
        self.N = 30
        self.NUM_REDUCERS = 2

    def mapper_init(self):

        def makeKeyHash(key, num_reducers):
            byteof = lambda char: int(format(ord(char), 'b'), 2)
            current_hash = 0
            for c in key:
                current_hash = (current_hash * 31 + byteof(c))
            return current_hash % num_reducers

        # printable ascii characters, starting with 'A'
        keys = [str(chr(i)) for i in range(65,65+self.NUM_REDUCERS)]
        partitions = []

        for key in keys:
            partitions.append([key, makeKeyHash(key, self.NUM_REDUCERS)])

        parts = sorted(partitions,key=itemgetter(1))
        self.partition_keys = list(np.array(parts)[:,0])

        self.partition_file = np.arange(0,self.N,self.N/(self.NUM_REDUCERS))[::-1]
        
        print((keys, partitions, parts, self.partition_keys, self.partition_file), file=stderr)

        
    def mapper(self, _, lines):
        terms, term_count, page_count, book_count = lines.split("\t")
        terms = terms.split()
        term_count = int(term_count)
        
        for item in terms:
            yield (item, term_count)
            
        for item in ["A", "B", "H", "I"]:
            yield (item, 0)
            
    def reducer_init(self):
        self.reducer_unique_key = int(random()*900000+100000)

    
    def reducer(self, keys, values):
        yield (self.reducer_unique_key, (keys, sum(values)))

        
if __name__ == "__main__":
    CustomPartitioner.run()