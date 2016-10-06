
from mrjob.job import MRJob

class GlobalState(MRJob):
    GlobalList = []
    
    def mapper(self, _, lines):
        if "wi" in lines:
            self.GlobalList.append(lines[2:8])
        yield (lines, 1)
        
    def reducer(self, values, counts):
        pass
    
    def reducer_final(self):
        yield(self.GlobalList, 1)

    
if __name__ == "__main__":
    GlobalState.run()