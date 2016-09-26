
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import sys

def order_key(order_in_reducer, key_name):
    number_of_stars = order_in_reducer//10 + 1
    number = str(order_in_reducer%10)
    return "%s %s" % ("*"*number_of_stars+number, key_name)

class Top50(MRJob):

    MRJob.SORT_VALUES = True
        
    def mapper_get_issue(self, _, lines):
        terms = list(csv.reader([lines]))[0]
        issue = terms[3]
        if issue == "":
            issue = "<blank>"
        yield (issue, 1)
    
    def combiner_count_issues(self, word, count):
        yield (word, sum(count))
        
    def reducer_init_totals(self):
        self.issue_counts = []
    
    def reducer_count_issues(self, word, count):
        issue_count = sum(count)
        self.issue_counts.append(int(issue_count))
        yield (word, issue_count)
        
    def reducer_final_emit_counts(self):
        yield (order_key(1, "Total"), sum(self.issue_counts))
        yield (order_key(2, "40th"), sorted(self.issue_counts)[-40])
    
    def reducer_init(self):
        self.increment_counter("Reducers", "Count", 1)
        self.var = {}
    
    def reducer(self, word, count):
        if word.startswith("*"):
            _, term = word.split()
            self.var[term] = next(count)

        else:
            total = sum(count)
            if total >= self.var["40th"]:
                yield (word, (total/self.var["Total"], total))
                
    def mapper_sort(self, key, value):
        value[0] = 1-float(value[0])
        yield value, key
        
    def reducer_sort(self, key, value):
        key[0] = round(1-float(key[0]),3)
        yield key, next(value)

    def steps(self):
        mr_steps = [MRStep(mapper=self.mapper_get_issue,
                           combiner=self.combiner_count_issues,
                           reducer_init=self.reducer_init_totals,
                           reducer=self.reducer_count_issues,
                           reducer_final=self.reducer_final_emit_counts),
                    MRStep(reducer_init=self.reducer_init,
                           reducer=self.reducer),
                    MRStep(mapper=self.mapper_sort,
                           reducer=self.reducer_sort)
                   ]
        return mr_steps
    
        
        
if __name__ == "__main__":
    Top50.run()