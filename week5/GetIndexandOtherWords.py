
import heapq
from re import findall
from mrjob.job import MRJob
from mrjob.step import MRStep


class TopList(list):
    def __init__(self, max_size, num_position=0):
        """
        Just like a list, except the append method adds the new value to the 
        list only if it is larger than the smallest value (or if the size of 
        the list is less than max_size). 
        
        If each element of the list is an int or float, uses that value for 
        comparison. If the elements in the list are lists or tuples, uses the 
        list_position element of the list or tuple for the comparison.
        """
        self.max_size = max_size
        self.pos = num_position
        
    def _get_key(self, x):
        return x[self.pos] if isinstance(x, (list, tuple)) else x
        
    def append(self, val):
        if len(self) < self.max_size:
            heapq.heappush(self, val)
        elif self._get_key(self[0]) < self._get_key(val):
            heapq.heapreplace(self, val)
            
    def final_sort(self):
        return sorted(self, key=self._get_key, reverse=True)

    
class GetIndexandOtherWords(MRJob):
    """
    Usage: python GetIndexandOtherWords.py --index-range 9000-10000 --top-n-words 10000 --use-term-counts True
    
    Given n-gram formatted data, outputs a file of the form:
    
    index    term
    index    term
    ...
    word     term
    word     term
    ...
    
    Where there would be 1001 index words and 10000 total words. Each word would be ranked based
    on either the term count listed in the Google n-gram data (i.e. the counts found in the
    underlying books) or the ranks would be based on the word count of the n-grams in the actual
    dataset (i.e. ignore the numbers/counts associated with each n-gram and count each n-gram
    exactly once).
    """
    def configure_options(self):
        super(GetIndexandOtherWords, self).configure_options()
        
        self.add_passthrough_option(
            '--index-range', 
            default="9-10", 
            help="Specify the range of the index words. ex. 9-10 means the ninth and " +
                 "tenth most popular words will serve as the index")
        
        self.add_passthrough_option(
            '--top-n-words', 
            default="10", 
            help="Specify the number of words to output in all")
        
        self.add_passthrough_option(
            '--use-term-counts', 
            default="True", 
            choices=["True","False"],
            help="When calculating the most frequent words, choose whether to count " + 
            "each word based on the term counts reported by Google or just based on " + 
            "the number of times the word appears in an n-gram")
        
        self.add_passthrough_option(
            '--return-counts', 
            default="False", 
            choices=["True","False"],
            help="The final output includes the counts of each word")
        
    def mapper_init(self):
        # Ensure command line options are sane
        top_n_words = int(self.options.top_n_words)
        last_index_word = int(self.options.index_range.split("-")[1])
        if top_n_words < last_index_word:
            raise ValueError("""--top-n-words value (currently %d) must be equal to or greater than
                             --index-range value (currently %d).""" % (top_n_words, last_index_word))
        
        self.stop_words =  {'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 
                            'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 
                            'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 
                            'its', 'itself', 'they', 'them', 'their', 'theirs', 
                            'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 
                            'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 
                            'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 
                            'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 
                            'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 
                            'with', 'about', 'against', 'between', 'into', 'through', 
                            'during', 'before', 'after', 'above', 'below', 'to', 'from', 
                            'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 
                            'again', 'further', 'then', 'once', 'here', 'there', 'when', 
                            'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 
                            'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 
                            'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 
                            'can', 'will', 'just', 'don', 'should', 'now'}
        
    def mapper(self, _, lines):
        terms, term_count, page_count, book_count = lines.split("\t")
        
        # Either use the ngram term count for the count or count each word just once
        if self.options.use_term_counts == "True":
            term_count = int(term_count)
        else:
            term_count = 1
        
        # Iterate through each term. Skip stop words
        for term in findall(r'[a-z]+', terms.lower()):
            if term in self.stop_words:
                pass
            else:
                yield (term, term_count)
        
    def combiner(self, term, counts):
        yield (term, sum(counts))      
        
        
    def reducer_init(self):
        """
        Accumulates the top X words and yields them. Note: should only use if
        you want to emit a reasonable amount of top words (i.e. an amount that 
        could fit on a single computer.)
        """
        self.top_n_words = int(self.options.top_n_words)
        self.TopTerms = TopList(self.top_n_words, num_position=1)
        
    def reducer(self, term, counts):
        self.TopTerms.append((term, sum(counts)))

    def reducer_final(self):
        for pair in self.TopTerms:
            yield pair
        
    def mapper_single_key(self, term, count):
        """
        Send all the data to a single reducer
        """
        yield (1, (term, count))
        
    def reducer_init_top_vals(self):
        # Collect top words
        self.top_n_words = int(self.options.top_n_words)
        self.TopTerms = TopList(self.top_n_words, num_position=1)
        # Collect index words
        self.index_range = [int(num) for num in self.options.index_range.split("-")]
        self.index_low, self.index_high = self.index_range
        # Control if output shows counts or just words
        self.return_counts = self.options.return_counts == "True"

    def reducer_top_vals(self, _, terms):
        for term in terms:
            self.TopTerms.append(term)
            
    def reducer_final_top_vals(self):
        TopTerms = self.TopTerms.final_sort()
        
        if self.return_counts:            
            # Yield index words
            for term in TopTerms[self.index_low-1:self.index_high]:
                yield ("index", term)

            # Yield all words
            for term in TopTerms:
                yield ("words", term)
        else:
            # Yield index words
            for term in TopTerms[self.index_low-1:self.index_high]:
                yield ("index", term[0])

            # Yield all words
            for term in TopTerms:
                yield ("words", term[0])
    
    def steps(self):
        """
        Step one: Yield top n-words from each reducer. Means dataset size is 
                  n-words * num_reducers. Guarantees overall top n-words are 
                  sent to the next step.
        
        """
        mr_steps = [MRStep(mapper_init=self.mapper_init,
                           mapper=self.mapper,
                           combiner=self.combiner,
                           reducer_init=self.reducer_init,
                           reducer_final=self.reducer_final,
                           reducer=self.reducer),
                    MRStep(mapper=self.mapper_single_key,
                           reducer_init=self.reducer_init_top_vals,
                           reducer=self.reducer_top_vals,
                           reducer_final=self.reducer_final_top_vals)]
        return mr_steps
        
if __name__ == "__main__":
    GetIndexandOtherWords.run()