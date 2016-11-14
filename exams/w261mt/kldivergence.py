# coding: utf-8

from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import numpy as np

class kldivergence(MRJob):
    # process each string character by character
    # the relative frequency of each character emitting Pr(character|str)
    # for input record 1.abcbe
    # emit "a"    [1, 0.2]
    # emit "b"    [1, 0.4] etc...
    def mapper1(self, _, line):
        index = int(line.split('.',1)[0])
        letter_list = re.sub(r"[^A-Za-z]+", '', line).lower()
        count = {}
        for l in letter_list:
            if count.has_key(l):
                count[l] += 1
            else:
                count[l] = 1
        for key in count:
            yield key, [index, count[key]*1.0/len(letter_list)]

    # on a component i calculate (e.g., "b")
    # Kullback–Leibler divergence of Q from P is defined as (P(i) log (P(i) / Q(i))
    def reducer1(self, key, values):
        p = 0
        q = 0
        for v in values:
            if v[0] == 1:  #String 1
                p = v[1]
            else:          # String 2
                q = v[1]
            
            if p and q:
                yield (None, p*np.log(p/q))
#         yield INSERT CODE

    #Aggegate components            
    def reducer2(self, key, values):
        kl_sum = 0
        for value in values:
            kl_sum = kl_sum + value
        yield "KLDivergence", kl_sum
            
    def steps(self):
        mr_steps = [self.mr(mapper=self.mapper1,
                        reducer=self.reducer1),
                
                    self.mr(reducer=self.reducer2)]
#         mr_steps = [MRStep(mapper=self.mapper1, reducer=self.reducer1)]
        return mr_steps

if __name__ == '__main__':
    kldivergence.run()