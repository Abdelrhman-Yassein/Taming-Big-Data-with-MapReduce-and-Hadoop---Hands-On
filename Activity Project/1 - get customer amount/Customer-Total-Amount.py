# -*- coding: utf-8 -*-
"""
Created on Wed Sep 29 17:28:21 2021

@author: Abdelrhman
"""

from mrjob.job import MRJob

class MRCustomerTotalAmount(MRJob):
    
    def mapper(self,_,line):
        (customer,item,amount) = line.split(',')
        yield customer,float(amount)
        
    def reducer(self,customer,amount):
        yield customer,sum(amount)
        
        
if __name__ == '__main__':
    MRCustomerTotalAmount.run()      
        