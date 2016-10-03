
from mrjob.job import MRJob
# Avoid broken pipe error
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 

class RJ(MRJob):
    def mapper_init(self):
        self.urls_used = set()
        self.urls = {}
        with open("urls.txt") as urls:
            for line in urls:
                url, key = line.strip().replace('"',"").split(",")
                self.urls[key] = url
        
    def mapper(self, _, lines):
        try:
            url = lines[2:6]
            yield (self.urls[url], lines)
            self.urls_used.add(url)
            
        except ValueError:
            pass
        
    def mapper_final(self):
        for key, value in self.urls.items():
            if key not in self.urls_used:
                yield (self.urls[key], "*")
                
    
                
    def reducer(self, url, values):
        quick_stash = 0
        for val in values:
            if val != "*":
                quick_stash += 1
                yield (url, val)
        if quick_stash == 0:
            yield (url, "None")
        
if __name__ == "__main__":
    RJ.run()