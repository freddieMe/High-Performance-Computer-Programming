#!/bin/bash
#SBATCH --time=00:05:00
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH -o 1n1c.txt
module load Python/3.4.3-goolf-2015a
mpiexec python cake.py
