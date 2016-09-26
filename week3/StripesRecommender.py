
from mrjob.job import MRJob
from collections import Counter
import sys
import heapq

def all_itemsets_of_size_two_stripes(array, key=None):
    """
    Generator that yields all valid itemsets of size two
    where each combo is as a stripe.
    
    key = None defaults to standard sorting.
    """
    array = sorted(array, key=key)
    for index, item in enumerate(array[:-1]):
        yield (item, {key:1 for key in array[index+1:]})

class TopList(list):
    def __init__(self, max_size):
        """
        Just like a list, except the append method adds the new value to the 
        list only if it is larger than the smallest value (or if the size of 
        the list is less than max_size). If each element of the list is an int
        or float, uses that value for comparison. If the first element is a 
        list or tuple, uses the first element of the list or tuple for the 
        comparison.
        """
        self.max_size = max_size
        
    def _get_key(self, x):
        return x[0] if isinstance(x, (list, tuple)) else x
        
    def append(self, val):
        key=lambda x: x[0] if isinstance(x, (list, tuple)) else x
        if len(self) < self.max_size:
            heapq.heappush(self, val)
        elif self._get_key(self[0]) < self._get_key(val):
            heapq.heapreplace(self, val)
            
    def final_sort(self):
        return sorted(self, key=self._get_key, reverse=True)
    
        
class StripesRecommender(MRJob):
    
    def mapper_init(self):
        self.basket_count = 0
    
    def mapper(self, _, lines):
        self.basket_count += 1
        products = lines.split()
        for item, value in all_itemsets_of_size_two_stripes(products):
            yield item, value
            
    def mapper_final(self):
        yield ("*** Total", {"total": self.basket_count})
        
    def combiner(self, keys, values):
        values_sum = Counter()
        for val in values:
            values_sum += Counter(val)
        yield keys, dict(values_sum)
    
    def reducer_init(self):
        self.top = TopList(50)
    
    def reducer(self, keys, values):
        values_sum = Counter()
        for val in values:
            values_sum += Counter(val)

        if keys == "*** Total":            
            self.total = values_sum["total"]
        else:
            for k, v in values_sum.items():
                if v >= 100:
                    self.top.append([v, round(v/self.total,3), keys+" "+k])

    def reducer_final(self):
        for count, perc, key in self.top.final_sort():
            yield key, (count, perc)
                    
if __name__ == "__main__":
    StripesRecommender.run()