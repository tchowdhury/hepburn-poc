#!/usr/bin/env python3
import subprocess
import sys
import os

def build_layer():
    """Build JWT layer by installing dependencies"""
    layer_dir = os.path.dirname(os.path.abspath(__file__))
    python_dir = os.path.join(layer_dir, "python")
    
    # Install dependencies
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", 
        "-r", os.path.join(layer_dir, "requirements.txt"),
        "-t", python_dir
    ])
    
    print(f"JWT layer built successfully in {python_dir}")

if __name__ == "__main__":
    build_layer()