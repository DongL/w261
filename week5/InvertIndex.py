
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from collections import Counter

class InvertIndex(MRJob):
    MRJob.input_protocol = JSONProtocol
    
    def mapper(self, key, words):
        n_words = len(words)
        
        for word in words: 
            yield (word, {key:n_words})
            
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
    InvertIndex.run()