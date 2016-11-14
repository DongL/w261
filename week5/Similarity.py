
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from itertools import combinations

class Similarity(MRJob):
    MRJob.input_protocol = JSONProtocol
    
    def mapper(self, key_term, docs):
        """
        Make co-occurrence keys for each pair of documents in the inverted
        index and make keys representing each document.
        """
        doc_names = docs.keys()
        for doc_pairs in combinations(sorted(list(doc_names)), 2):
            yield (doc_pairs, 1)
        for name in doc_names:
            yield (name, 1)
            
    def combiner(self, key, value):
        yield (key, sum(value))
        
        
    ### Custom partitioner code goes here
    def reducer_init(self):
        self.words = {}
        self.results = []
    
    def reducer(self, doc_or_docs, count):
        if isinstance(doc_or_docs, str):
            self.words[doc_or_docs] = sum(count)
        else:
            d1, d2 = doc_or_docs
            d1_n_words, d2_n_words = self.words[d1], self.words[d2]
            intersection = float(sum(count))
            
            jaccard = round(intersection/(d1_n_words + d2_n_words - intersection), 3)
            cosine = round(intersection/(d1_n_words**.5 * d2_n_words**.5), 3)
            dice = round(2*intersection/(d1_n_words + d2_n_words), 3)
            overlap = round(intersection/min(d1_n_words, d2_n_words), 3)
            average = round(sum([jaccard, cosine, dice, overlap])/4.0, 3)
            
            self.results.append([doc_or_docs, {"jacc":jaccard, "cos":cosine, 
                                               "dice":dice, "ol":overlap, "ave":average}])
            
    def reducer_final(self):
        for doc, result in sorted(self.results, key=lambda x: x[1]["ave"], reverse=True):
            yield (doc, result)
        
if __name__ == "__main__":
    Similarity.run()