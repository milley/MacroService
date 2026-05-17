// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('@grpc/grpc-js');
var calculator_pb = require('./calculator_pb.js');

function serialize_calculator_AddRequest(arg) {
  if (!(arg instanceof calculator_pb.AddRequest)) {
    throw new Error('Expected argument of type calculator.AddRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_calculator_AddRequest(buffer_arg) {
  return calculator_pb.AddRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_calculator_AddResponse(arg) {
  if (!(arg instanceof calculator_pb.AddResponse)) {
    throw new Error('Expected argument of type calculator.AddResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_calculator_AddResponse(buffer_arg) {
  return calculator_pb.AddResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_calculator_Number(arg) {
  if (!(arg instanceof calculator_pb.Number)) {
    throw new Error('Expected argument of type calculator.Number');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_calculator_Number(buffer_arg) {
  return calculator_pb.Number.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_calculator_PrimeResponse(arg) {
  if (!(arg instanceof calculator_pb.PrimeResponse)) {
    throw new Error('Expected argument of type calculator.PrimeResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_calculator_PrimeResponse(buffer_arg) {
  return calculator_pb.PrimeResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_calculator_PrimesRequest(arg) {
  if (!(arg instanceof calculator_pb.PrimesRequest)) {
    throw new Error('Expected argument of type calculator.PrimesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_calculator_PrimesRequest(buffer_arg) {
  return calculator_pb.PrimesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_calculator_SumResponse(arg) {
  if (!(arg instanceof calculator_pb.SumResponse)) {
    throw new Error('Expected argument of type calculator.SumResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_calculator_SumResponse(buffer_arg) {
  return calculator_pb.SumResponse.deserializeBinary(new Uint8Array(buffer_arg));
}


// 计算器服务 - 演示不同类型的 gRPC 调用
var CalculatorService = exports.CalculatorService = {
  // 简单一元调用：加法
add: {
    path: '/calculator.Calculator/Add',
    requestStream: false,
    responseStream: false,
    requestType: calculator_pb.AddRequest,
    responseType: calculator_pb.AddResponse,
    requestSerialize: serialize_calculator_AddRequest,
    requestDeserialize: deserialize_calculator_AddRequest,
    responseSerialize: serialize_calculator_AddResponse,
    responseDeserialize: deserialize_calculator_AddResponse,
  },
  // 服务端流式：生成质数序列
streamPrimes: {
    path: '/calculator.Calculator/StreamPrimes',
    requestStream: false,
    responseStream: true,
    requestType: calculator_pb.PrimesRequest,
    responseType: calculator_pb.PrimeResponse,
    requestSerialize: serialize_calculator_PrimesRequest,
    requestDeserialize: deserialize_calculator_PrimesRequest,
    responseSerialize: serialize_calculator_PrimeResponse,
    responseDeserialize: deserialize_calculator_PrimeResponse,
  },
  // 客户端流式：计算多个数字的和
sumStream: {
    path: '/calculator.Calculator/SumStream',
    requestStream: true,
    responseStream: false,
    requestType: calculator_pb.Number,
    responseType: calculator_pb.SumResponse,
    requestSerialize: serialize_calculator_Number,
    requestDeserialize: deserialize_calculator_Number,
    responseSerialize: serialize_calculator_SumResponse,
    responseDeserialize: deserialize_calculator_SumResponse,
  },
};

exports.CalculatorClient = grpc.makeGenericClientConstructor(CalculatorService, 'Calculator');
