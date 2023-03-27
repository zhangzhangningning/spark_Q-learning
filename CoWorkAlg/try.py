import workload
import os.path
import sys
with open('/opt/share/CoWorkAlg/ColSelect.txt','r') as f:
    lines = f.readlines()
    last_line = lines[-1]

ColSelect = eval(last_line)
ZorderCol = []
for i in range(len(ColSelect)):
    if ColSelect[i] == 1:
        ZorderCol.append(workload.orgin_col[i])

print(ZorderCol)

for i in range(len(workload.workload1)):
    sql_sentence = workload.workload1[i]
    print(sql_sentence)