
import sys
import re
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol, TextValueProtocol

# Prevents broken pipe errors from using ... | head
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 

def sum_hs(counts):
    h_total, s_total = 0, 0
    for h, s in counts:
        h_total += h
        s_total += s
    return (h_total, s_total)


class NaiveBayes(MRJob):
    MRJob.OUTPUT_PROTOCOL = TextValueProtocol

    def mapper(self, _, lines):
        _, spam, subject, email = lines.split("\t")
        words = re.findall(r'[a-z]+', (email.lower()+" "+subject.lower()))
        
        if spam == "1":
            h, s = 0, 1
        else:
            h, s = 1, 0        
        yield "***Total Emails", (h, s)
        
        for word in words:
            yield word, (h, s)
            yield "***Total Words", (h, s)
                    
    def combiner(self, key, count):
        yield key, sum_hs(count)
    
    def reducer_init(self):
        self.total_ham = 0
        self.total_spam = 0
        
    def reducer(self, key, count):
        ham, spam = sum_hs(count)
        if key.startswith("***"):
            if "Words" in key:
                self.total_ham, self.total_spam = ham, spam
            elif "Emails" in key:
                total = ham + spam
                yield "_", "***Priors\t%.10f\t%.10f" % (ham/total, spam/total)
        else:
            pg_ham, pg_spam = ham/self.total_ham, spam/self.total_spam
            yield "_", "%s\t%.10f\t%.10f" % (key, pg_ham, pg_spam)
    
if __name__ == "__main__":
    NaiveBayes.run()