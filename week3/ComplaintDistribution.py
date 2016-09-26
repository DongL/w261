from mrjob.job import MRJob
from mrjob.step import MRStep

class ComplaintDistribution(MRJob):
    def mapper(self, _, lines):
        line = lines[:30]
        if "Debt collection" in line:
            self.increment_counter('Complaint', 'Debt collection', 1)
        elif "Mortgage" in line:
            self.increment_counter('Complaint', 'Mortgage', 1)
        else:
            self.increment_counter('Complaint', 'Other', 1)
            
if __name__ == "__main__":
    ComplaintDistribution.run()