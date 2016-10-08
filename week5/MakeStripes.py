
from mrjob.job import MRJob
from collections import Counter

class MakeStripes(MRJob):
    def mapper(self, _, lines):
        terms, term_count, page_count, book_count = lines.split("\t")
        terms = terms.split()
        term_count = int(term_count)
        
        for item in terms:
            yield (item, {term:term_count for term in terms if term != item})
        
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