// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::io::Read;
use std::sync::Arc;
use jni::{
    errors::Result as JniResult,
    objects::{JClass, JMethodID},
    signature::{Primitive, ReturnType},
    JNIEnv,
};
use jni::objects::GlobalRef;
use crate::errors::CometResult;
use crate::jvm_bridge::JVMClasses;

/// A struct that holds all the JNI methods and fields for JVM `InputStream` class.
#[allow(dead_code)] // we need to keep references to Java items to prevent GC
pub struct CometIORead<'a> {
    pub class: JClass<'a>,
    pub method_read: JMethodID,
    pub method_read_ret: ReturnType,
    pub method_close: JMethodID,
    pub method_close_ret: ReturnType,
}

impl<'a> CometIORead<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/jni/CometIORead";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<CometIORead<'a>> {
        let class = env.find_class(Self::JVM_CLASS)?;

        Ok(CometIORead {
            class,
            method_read: env.get_method_id(Self::JVM_CLASS, "read", "(JI)I")?,
            method_read_ret: ReturnType::Primitive(Primitive::Int),
            method_close: env.get_method_id(Self::JVM_CLASS, "close", "()V")?,
            method_close_ret: ReturnType::Primitive(Primitive::Int),
        })
    }
}

pub struct CometIOReadWrapper {
    inner: Arc<GlobalRef>,
}

impl CometIOReadWrapper {
    pub fn new(inner: Arc<GlobalRef>) -> Self {
        Self { inner }
    }

    fn read(&mut self, buf_ptr: jni::sys::jlong, buf_len: jni::sys::jint) -> CometResult<i32> {
        let mut env = JVMClasses::get_env()?;
        unsafe {
            jni_call!(&mut env, comet_io_read(self.inner.as_obj()).read(buf_ptr, buf_len) -> i32)
        }
    }

    fn close(&mut self) -> CometResult<()> {
        let mut env = JVMClasses::get_env()?;
        unsafe {
            jni_call!(&mut env, comet_io_read(self.inner.as_obj()).close() -> ())
        }
    }
}

impl Drop for CometIOReadWrapper {
    fn drop(&mut self) {
        self.close().unwrap()
    }
}

impl Read for CometIOReadWrapper {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_ptr = buf.as_mut_ptr() as jni::sys::jlong;
        let buf_len = buf.len() as jni::sys::jint;

        match self.read(buf_ptr, buf_len) {
            Ok(i) => {
                if i == 0 {
                    self.close().map_err(|e| e.into())?;
                }
                Ok(i as usize)
            }
            Err(e) => Err(e.into())
        }
    }
}