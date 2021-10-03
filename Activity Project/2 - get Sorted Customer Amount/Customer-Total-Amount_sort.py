# -*- coding: utf-8 -*-
"""
Created on Wed Sep 29 17:49:30 2021

@author: Abdelrhman
"""


from mrjob.job import MRJob
from mrjob.job import MRStep

class MRCustomerTotalAmountSort(MRJob):
    
    def steps(self):
        return[
                MRStep(mapper=self.mapper_get_customer,
                       reducer=self.reducer_get_customer_amount),
                MRStep(mapper=self.mapper_get_amount_sum_key,
                       reducer=self.reducer_output_amount)
            ]
    def mapper_get_customer(self,_,line):
        (customer,item,amount) = line.split(',')
        yield customer,float(amount)
        
    def reducer_get_customer_amount(self,customer,amount):
        yield customer,sum(amount)
    
    def mapper_get_amount_sum_key(self,customer,amount):
        yield '%04.02f'%float(amount) ,customer
        
    def reducer_output_amount(self,amount,customers):
        for customer in customers:
            yield customer,amount
        
if __name__ == '__main__':
    MRCustomerTotalAmountSort.run()      
        