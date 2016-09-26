
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import sys

class IssueCounterCombiner(MRJob):
    
    def mapper(self, _, lines):
        self.increment_counter("Mappers", "Tasks", 1)
        terms = list(csv.reader([lines]))[0]
        yield (terms[3], 1)
    
    def combiner(self, word, count):
        self.increment_counter("Combiners", "Tasks", 1)
        yield (word, sum(count))
    
    def reducer(self, word, count):
        self.increment_counter("Reducers", "Tasks", 1)
        self.increment_counter("Reducers", "Lines processed", len(list(count)))
        yield (word, sum(count))
        
if __name__ == "__main__":
    IssueCounterCombiner.run()