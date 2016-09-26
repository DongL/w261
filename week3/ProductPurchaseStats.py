
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import heapq


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


class ProductPurchaseStats(MRJob):
    
    def mapper_init(self):
        self.largest_basket = 0
        self.total_items = 0
    
    def mapper(self, _, lines):
        products = lines.split()
        n_products = len(products)
        self.total_items += n_products
        if n_products > self.largest_basket:
            self.largest_basket = n_products
        for prod in products:
            yield (prod, 1)
            
    def mapper_final(self):
        self.increment_counter("product stats", "largest basket", self.largest_basket)
        yield ("*** Total", self.total_items)
        
    def combiner(self, keys, values):
        yield keys, sum(values)
        
    def reducer_init(self):
        self.top50 = TopList(50)
        self.total = 0
        
    def reducer(self, key, values):
        value_count = sum(values)
        
        if key == "*** Total":
            self.total = value_count
        else:
            self.increment_counter("product stats", "unique products")
            self.top50.append([value_count, value_count/self.total, key])

    def reducer_final(self):
        for counts, relative_rate, key in self.top50.final_sort():
            yield key, (counts, round(relative_rate,3))
    
if __name__ == "__main__":
    ProductPurchaseStats.run()