
from mrjob.job import MRJob

# Avoid broken pipe error
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 

class LJ(MRJob):
    def mapper_init(self):
        self.urls = {}
        with open("urls.txt") as urls:
            for line in urls:
                url, key = line.strip().replace('"',"").split(",")
                self.urls[key] = url
        
    def mapper(self, _, lines):
        try:
            yield (lines, self.urls[lines[2:6]])
        except ValueError:
            yield (lines, "")
        
if __name__ == "__main__":
    LJ.run()