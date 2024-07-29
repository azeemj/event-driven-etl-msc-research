import logging as log
import new_math as m

#from new_math import CustomMath

list_a =[1,2,4,9,2,1]

list_a.append(100)

print(list_a)

log.info('test')
log.error('error')

my_math = m.CustomMath("Aaa", 232)
print(my_math.gt_name())

data = my_math.file_reader('./temp_test/a.txt')

# for my_data in data:
#     print(my_data)