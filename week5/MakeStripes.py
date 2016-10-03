
from mrjob.job import MRJob
from collections import Counter

class MakeStripes(MRJob):
    def mapper_init(self):
        self.stripes = {}
    
    def mapper(self, _, lines):
        terms, term_count, page_count, book_count = lines.split("\t")
        terms = terms.split()
        term_count = int(term_count)
        
        for item in terms:
            yield (item, {val:term_count for val in terms if val != item})
        
    def combiner(self, keys, values):
        values_sum = Counter()
        for val in values:
            values_sum += Counter(val)
        yield keys, dict(values_sum)

    def reducer(self, keys, values):
        values_sum = Counter()
        for val in values:
            values_sum += Counter(val)
        yield keys, dict(values_sum)
        
if __name__ == "__main__":
    MakeStripes.run()