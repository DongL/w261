
from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq
import sys

def all_itemsets_of_size_two(array, key=None, return_type="string", concat_val=" "):
    """
    Generator that yields all valid itemsets of size two
    where each combo is returned in an order sorted by key.
    
    key = None defaults to standard sorting.
    
    return_type: can be "string" or "tuple". If "string", 
    concatenates values with concat_val and returns string.
    If tuple, returns a tuple with two elements.
    """
    array = sorted(array, key=key)
    for index, item in enumerate(array):
        for other_item in array[index:]:
            if item != other_item:
                if return_type == "string":
                    yield "%s%s%s" % (str(item), concat_val, str(other_item))
                else:
                    yield (item, other_item) 

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
    
                    
class PairsRecommender(MRJob):
    def mapper_init(self):
        self.total_baskets = 0
    
    def mapper(self, _, lines):
        self.total_baskets += 1
        products = lines.split()
        self.increment_counter("job stats", "number of items", len(products))
        for itemset in all_itemsets_of_size_two(products):
            self.increment_counter("job stats", "number of item combos")
            yield (itemset, 1)
            
    def mapper_final(self):
        self.increment_counter("job stats", "number of baskets", self.total_baskets)
        yield ("*** Total", self.total_baskets)
        
    def combiner(self, key, values):
        self.increment_counter("job stats", "number of keys fed to combiner")
        yield key, sum(values)
    
    def reducer_init(self):
        self.top_values = TopList(50)
        self.total_baskets = 0
    
    def reducer(self, key, values):
        values_sum = sum(values)
        if key == "*** Total":
            self.total_baskets = values_sum
        elif values_sum >= 100:
            self.increment_counter("job stats", "number of itemsets >= 100")
            basket_percent = values_sum/self.total_baskets
            self.top_values.append([values_sum, round(basket_percent,3), key])
        else:
            self.increment_counter("job stats", "number of itemsets < 100")
            
    def reducer_final(self):
        for values_sum, basket_percent, key in self.top_values.final_sort():
            yield key, (values_sum, basket_percent)
        
if __name__ == "__main__":
    PairsRecommender.run()