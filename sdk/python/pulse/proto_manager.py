import os
import sys
import urllib.request
import re

def ensure_proto(host="localhost", port=5555):
    try:
        from grpc_tools import protoc
    except ImportError:
        return

    current_dir = os.path.dirname(os.path.abspath(__file__))
    proto_dir = os.path.join(current_dir, "proto")
    proto_file = os.path.join(proto_dir, "pulse.proto")
    
    # Try to download
    try:
        url = f"http://{host}:{port}/proto"
        # Short timeout to avoid blocking startup
        with urllib.request.urlopen(url, timeout=0.5) as response:
            content = response.read()
            
        if not os.path.exists(proto_dir):
            os.makedirs(proto_dir)
            
        # Check if content changed? For now just overwrite.
        with open(proto_file, "wb") as f:
            f.write(content)
            
        # Compile
        cmd = [
            "grpc_tools.protoc",
            f"-I{proto_dir}",
            f"--python_out={proto_dir}",
            f"--grpc_python_out={proto_dir}",
            "pulse.proto"
        ]
        
        # We run from current_dir to avoid path issues?
        # Actually, let's just pass the args.
        exit_code = protoc.main(cmd)
        
        if exit_code == 0:
            # Fix imports in pulse_pb2_grpc.py to use relative import
            grpc_file = os.path.join(proto_dir, "pulse_pb2_grpc.py")
            if os.path.exists(grpc_file):
                with open(grpc_file, "r") as f:
                    content = f.read()
                
                # Replace 'import pulse_pb2' with 'from . import pulse_pb2'
                new_content = re.sub(r'^import pulse_pb2', 'from . import pulse_pb2', content, flags=re.MULTILINE)
                
                if new_content != content:
                    with open(grpc_file, "w") as f:
                        f.write(new_content)
                        
    except Exception:
        # Ignore errors (server down, etc)
        pass
