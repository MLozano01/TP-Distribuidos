import logging
import random
import subprocess
import sys
import time

SLEEP_SECONDS = 2

logging.basicConfig(level=logging.INFO)

def create_possible_names(prefixes):
	ps = subprocess.run(['docker', 'ps'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
	result = ps.stdout
	black_list = "healthchecker-1"

	possible_names = []
	for line in result.split("\n")[1::]:
		values = line.split()
		if len(values) == 0:
			continue
		name = values[-1]
		if name != black_list and any([name.startswith(prefix) for prefix in prefixes]):
			possible_names.append(name)
	return possible_names
        
def set_mode():
	modes = ["CHAOS", "BOMB"]
	mode = ""
	if len(sys.argv) == 2:
		mode = sys.argv[1]
        
	if mode not in modes:
		mode = "CHAOS"

	return mode

def main():
	mode = set_mode()

	# name_prefix = ['transformer-',
	# 				  	'joiner-', 
	# 					'filter-', 
	# 					'aggregator-',
	# 					'data-controller-', 
	# 					'healthchecker-', 
	# 					'reducer-']

	name_prefix  =['joiner-']

	logging.info(f"Running in mode {mode}")
	if mode == "CHAOS":
		while True:
			possible_names = create_possible_names(name_prefix)
			container = random.choice(possible_names)

			result = subprocess.run(['docker', 'kill', container], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			logging.info(f'Command executed. Killed "{container}". Result={result.returncode}.')

			time.sleep(SLEEP_SECONDS)
	
	else:
		possible_names = create_possible_names(name_prefix)
		command = ['docker', 'kill'] + possible_names
		
		result = subprocess.run(command, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		logging.info(f'Command executed. Killed containers "{possible_names}". Result={result.returncode}.')



if __name__ == '__main__':
    main()