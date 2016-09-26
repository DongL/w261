
from IssueCounterCombiner import IssueCounterCombiner

mr_job = IssueCounterCombiner(args=['Temp_data/Consumer_Complaints.csv'])

with mr_job.make_runner() as runner:
    runner.run() 
    print(runner.counters())
#     for line in runner.stream_output(): 
#         print(mr_job.parse_output_line(line))