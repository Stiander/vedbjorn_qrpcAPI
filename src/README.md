
# Creating Python files :
You need to have a global install of gRPC tools : 
    cmd > python -m pip install grpcio-tools

Then generate the files :
    cmd > python -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. ./vedbjorn.proto