from __future__ import division
from mrjob.job import MRJob
import re
import numpy as np
class kldivergence_smooth(MRJob):
    
    # process each string character by character
    # the relative frequency of each character emitting Pr(character|str)
    # for input record 1.abcbe
    # emit "a"    [1, (1+1)/(5+24)]
    # emit "b"    [1, (2+1)/(5+24) etc...
    def mapper1(self, _, line):
        index = int(line.split('.',1)[0])
        letter_list = re.sub(r"[^A-Za-z]+", '', line).lower()
        count = {}
        
        # (ni+1)/(n+24)
        
        for l in letter_list:
            if count.has_key(l):
                count[l] += 1
            else:
                count[l] = 1
                
        for letter in ['q', 'j']:
            if letter not in letter_list:
                count[letter] = 0
            
        for key in count:
            yield key, [index, (1+count[key]*1.0)/(24+len(letter_list))]

    
    def reducer1(self, key, values):
        p = 0
        q = 0
        for v in values:
            if v[0] == 1:
                p = v[1]
            else:
                q = v[1]

        yield (None, p*np.log(p/q))                

    # Aggregate components             
    def reducer2(self, key, values):
        kl_sum = 0
        for value in values:
            kl_sum = kl_sum + value
        yield "KLDivergence", kl_sum
            
    def steps(self):
        return [self.mr(mapper=self.mapper1,
                        reducer=self.reducer1),
                self.mr(reducer=self.reducer2)
               
               ]

if __name__ == '__main__':
    kldivergence_smooth.run()