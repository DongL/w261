
import re
from mrjob.job import MRJob
from math import log

def tsv_model_to_dict(file):
    results = {}
    with open(file, "r") as model:
        for line in model:
            term, ham, spam = line.strip().split("\t")
            results[term] = (float(ham), float(spam))     
    return results

class NBPredict(MRJob):
    def mapper_init(self):
        try:
            self.model = tsv_model_to_dict("CHINESE_model.tsv")
        except FileNotFoundError:
            self.model = tsv_model_to_dict("SPAM_Model_MND.tsv")
    
    def mapper(self, _, lines):
        _, spam_actual, subject, email = lines.split("\t")
        words = re.findall(r'[a-z]+', (email.lower()+" "+subject.lower()))
        
        ham_prob, spam_prob = self.model["***Priors"]
        ham_prob, spam_prob = log(ham_prob), log(spam_prob)
        
        for word in words:
            ham, spam = self.model[word]
            if ham*spam == 0:
                ham += .0001
                spam += .0001
            ham_prob += log(ham)
            spam_prob += log(spam)
            
        result = 1 if ham_prob < spam_prob else 0
        
        spam_actual = int(spam_actual)
        
        yield spam_actual, result
        
    def reducer(self, key, value):
        predictions = 0
        correct = 0
        
        for val in value:
            if val == key:
                correct += 1
            predictions += 1
            
        accuracy = correct/predictions
        
        yield key, [correct, predictions, accuracy]

if __name__ == "__main__":
    NBPredict.run()