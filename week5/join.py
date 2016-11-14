
from mrjob.job import MRJob
from mrjob.step import MRStep

# Avoid broken pipe error
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 

class Join(MRJob):
    def configure_options(self):
        super(Join, self).configure_options()
        
        self.add_passthrough_option(
            '--join', 
            default="left", 
            help="Options: left, inner, right")
        
    def mapper_init(self):
        self.join = self.options.join
        self.urls_used = set()
        self.urls = {}
        
        try:
            open("urls.txt")
            filename = "urls.txt"
        except FileNotFoundError:
            filename = "limited_urls.txt"
        
        with open(filename) as urls:
            for line in urls:
                url, key = line.strip().replace('"',"").split(",")
                self.urls[key] = url
        
    def mapper(self, _, lines):
        try:
            url = lines[2:6]
            if self.join in ["inner", "left"]:
                yield (lines, self.urls[url])
            elif self.join in ["right"]:
                yield (self.urls[url], lines)
                self.urls_used.add(url)
            
        except KeyError:
            if self.join in ["inner", "right"]:
                pass
            else:
                yield (lines, "None")

    def mapper_final(self):
        for key, value in self.urls.items():
            if key not in self.urls_used:
                yield (self.urls[key], "*")
                
    def reducer(self, url, values):
        quick_stash = 0
        for val in values:
            if val != "*":
                quick_stash += 1
                yield (val, url)
        if quick_stash == 0:
            yield ("None", url)
            
    def steps(self):
        join = self.options.join
        if join in ["inner", "left"]:
            mrsteps = [MRStep(mapper_init=self.mapper_init,
                              mapper=self.mapper)]
        if join == "right":
            mrsteps = [MRStep(mapper_init=self.mapper_init,
                              mapper=self.mapper,
                              mapper_final=self.mapper_final,
                              reducer=self.reducer)]  
        return mrsteps
        
if __name__ == "__main__":
    Join.run()