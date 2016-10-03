#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import re    
import operator
 
class mostFrequentVisitors(MRJob):
    
    OUTPUT_PROTOCOL = RawValueProtocol
    
    URLs = {}

    def steps(self):
        return [MRStep(
                mapper = self.mapper,
                combiner = self.combiner,
                reducer_init = self.reducer_init,
                reducer = self.reducer
                )]
    
    def mapper(self, _, line):
        data = re.split(",",line)
        pageID = data[1]
        custID = data[4]
        yield pageID,{custID:1}
        
    def combiner(self,pageID,visits):
        allVisits = {}
        for visit in visits:
            for custID in visit.keys():
                allVisits.setdefault(custID,0)
                allVisits[custID] += visit[custID]
        yield pageID,allVisits
        
    def reducer_init(self):
        with open("anonymous-msweb.data", "r") as IF:
            for line in IF:
                try:
                    line = line.strip()
                    data = re.split(",",line)
                    URL = data[4]
                    pageID = data[1]
                    self.URLs[pageID] = URL
                except IndexError:
                    pass

    def reducer(self,pageID,visits):
        allVisits = {}
        for visit in visits:
            for custID in visit.keys():
                allVisits.setdefault(custID,0)
                allVisits[custID] += visit[custID]
        custID = max(allVisits.items(), key=operator.itemgetter(1))[0]
        yield None,self.URLs[pageID]+","+pageID+","+custID+","+str(allVisits[custID])
        
if __name__ == '__main__':
    mostFrequentVisitors.run()