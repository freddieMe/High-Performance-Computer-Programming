#!/bin/bash
#SBATCH --time=00:05:00
#SBATCH --partition=physical
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH -o 2n8c.txt
module load Python/3.4.3-goolf-2015a
mpiexec python cake.py