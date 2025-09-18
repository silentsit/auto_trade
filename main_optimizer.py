#!/usr/bin/env python3
"""
Main Optimizer Script
Handles system setup and optimization tasks
"""

import subprocess
import sys
import os
from typing import List, Optional

def run_command(command: List[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result"""
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=check)
        print(f"✅ Command executed successfully: {' '.join(command)}")
        if result.stdout:
            print(f"Output: {result.stdout}")
        return result
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed: {' '.join(command)}")
        print(f"Error: {e.stderr}")
        return e

def install_packages():
    """Install required system packages"""
    print("🔧 Installing required packages...")
    
    # Update package list
    run_command(["sudo", "apt-get", "update"])
    
    # Install Python packages
    packages = ["python3", "python3-pip", "python3-venv"]
    run_command(["sudo", "apt-get", "install", "-y"] + packages)
    
    print("✅ Package installation complete!")

def setup_virtual_environment():
    """Set up a Python virtual environment"""
    print("🐍 Setting up virtual environment...")
    
    # Create virtual environment
    if not os.path.exists("venv"):
        run_command(["python3", "-m", "venv", "venv"])
        print("✅ Virtual environment created!")
    else:
        print("ℹ️ Virtual environment already exists")
    
    # Activate and install requirements
    print("📦 Installing Python dependencies...")
    run_command(["venv/bin/pip", "install", "--upgrade", "pip"])
    
    # Install your requirements if they exist
    if os.path.exists("requirements.txt"):
        run_command(["venv/bin/pip", "install", "-r", "requirements.txt"])
        print("✅ Requirements installed!")
    else:
        print("ℹ️ No requirements.txt found")

def main():
    """Main function"""
    print("🚀 Starting main optimizer...")
    
    # Check if running as root
    if os.geteuid() == 0:
        print("⚠️ Running as root - be careful!")
    
    # Install system packages
    install_packages()
    
    # Setup Python environment
    setup_virtual_environment()
    
    print("🎉 Setup complete! You can now run your trading bot.")
    print("To activate the virtual environment: source venv/bin/activate")

if __name__ == "__main__":
    main() 