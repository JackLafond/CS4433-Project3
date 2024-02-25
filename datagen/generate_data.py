import random

def generate_people_data(file_path, num_points):
    with open(file_path, 'w') as file:
        for i in range(num_points):
            point_id = i + 1
            x = random.uniform(1, 10000)
            y = random.uniform(1, 10000)
            age = random.randint(13, 70)
            gender = random.choice(['Male', 'Female'])

            attributes = f"{age},{gender}"

            line = f"{point_id},{x:.2f},{y:.2f},{attributes}\n"
            file.write(line)

def generate_infected_subset(input_file, output_file, infection_probability):
    with open(input_file, 'r') as input_file:
        all_people = input_file.readlines()

    subset = [line for line in all_people if random.random() < infection_probability]

    with open(output_file, 'w') as output_file:
        for line in subset:
            output_file.write(line)

def generate_some_infected_large(input_people_file, input_infected_file, output_file):
    # Read all people records
    with open(input_people_file, 'r') as people_file:
        all_people = [line.strip() for line in people_file.readlines()]

    infected_ids = set()
    # Read infected records and create a set of infected IDs
    with open(input_infected_file, 'r') as infected_file:
        for line in infected_file:
            infected_ids.add(int(line.split(',')[0]))
    
    print(f"Number of infected people: {len(infected_ids)}")

    # Generate PEOPLE-SOME-INFECTED-large with "INFECTED" column
    with open(output_file, 'w') as output_file:
        for person_line in all_people:
            person_id = int(person_line.split(',')[0])
            infected_status = "yes" if person_id in infected_ids else "no"
            output_file.write(f"{person_line.strip()},{infected_status}\n")
            
            
# Generate PEOPLE-large
people_large_path = "./PEOPLE-large.txt"
num_points = 10000000
generate_people_data(people_large_path, num_points)
print(f"Generated {num_points} data points in {people_large_path}.")

# Generate INFECTED-small
infected_small_path = "INFECTED-small.txt"
generate_infected_subset(people_large_path, infected_small_path, 0.02)
print(f"Generated a subset of infected individuals in {infected_small_path}.")

# Generate PEOPLE-SOME-INFECTED-large
people_some_infected_large_path = "PEOPLE-SOME-INFECTED-large.txt"
generate_some_infected_large(people_large_path, infected_small_path, people_some_infected_large_path)
print(f"Generated {people_some_infected_large_path} with 'INFECTED' column.")