
#!/bin/bash
echo "Building Email Guardian for $(uname)..."
echo

# Install packaging requirements
echo "Installing packaging dependencies..."
pip install -r requirements_packaging.txt
if [ $? -ne 0 ]; then
    echo "Error installing dependencies"
    read -p "Press any key to continue..."
    exit 1
fi

# Run the packaging script  
echo "Running packaging script..."
python build_package.py

echo
echo "Build complete! Check the dist/EmailGuardian_Package folder."
read -p "Press any key to continue..."
