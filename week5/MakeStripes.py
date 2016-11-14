
from mrjob.job import MRJob
from collections import Counter
from sys import stderr
from re import findall

class MakeStripes(MRJob):
    def mapper_init(self):
        """
        Read in index words and word list.
        """
        self.stripes = {}
        
        self.indexlist, self.wordslist = [],[]
        with open('vocabs', 'r') as vocabFile:
            for line in vocabFile:
                word_type, word = line.replace('"', '').split()
                if word_type == 'index':
                    self.indexlist.append(word)
                else:
                    self.wordslist.append(word)
        
        # Convert to sets to make lookups faster
        self.indexlist = set(self.indexlist)
        self.wordslist = set(self.wordslist)
    
    def mapper(self, _, lines):
        """
        Make stripes using index and words list
        """
        terms, term_count, page_count, book_count = lines.split("\t")
        term_count = int(term_count)
        terms = findall(r'[a-z]+', terms.lower())
                
        for item in terms:
            if item in self.indexlist:
                for val in terms:
                    if val != item and val in self.wordslist:
                        yield item, {val:term_count}
        
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