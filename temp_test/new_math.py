
import logging as log
import os


class CustomMath:

    def __init__(self, name, age):
        self.name = name
        self.age = age


    def gt_name(self):
        return {'name': self.name , 'age': self.age}
    

    def file_reader(self, file_name = None):

        try:
            data =N
            if not file_name :
                log.error("No file name is given")
                return False
            
            if not os.path.isfile(file_name):
                log.error(f"File '{file_name}' does not exist")
                return False
            
            with open(file_name, 'r') as file:
                #log.info(f'data log: {file.readlines()}')
                data= file.readlines()
                file.seek(0)
                for ln in file:
                    print('cc', ln)

            return data
        except Exception as e:
            log.error(f'error {e}')
