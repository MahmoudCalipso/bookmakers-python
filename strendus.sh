#!/bin/bash
if [ $# -eq 0 ]
	then
		source /root/anaconda3/bin/activate updaters && cd scripts/Downloaders/Strendus && python run.py
else
	source /root/anaconda3/bin/activate updaters && cd scripts/Downloaders/Strendus && python run.py live
fi