
from mrjob.job import MRJob

class SimpleCounters(MRJob):
    def mapper_init(self):
        self.increment_counter("Mappers", "Count", 1)
    
    def mapper(self, _, lines):
        self.increment_counter("Mappers", "Tasks", 1)
        for word in lines.split():
            yield (word, 1)
    
    def reducer_init(self):
        self.increment_counter("Reducers", "Count", 1)
    
    def reducer(self, word, count):
        self.increment_counter("Reducers", "Tasks", 1)
        yield (word, sum(count))
        
if __name__ == "__main__":
    SimpleCounters.run()