#!/usr/bin/env python3
"""简单的文件上传测试脚本"""
import streamlit as st

st.title("文件上传测试")

uploaded_file = st.file_uploader("选择一个文件", type=None)

if uploaded_file is not None:
    st.write(f"文件名: {uploaded_file.name}")
    st.write(f"文件大小: {uploaded_file.size} bytes")
    st.write(f"文件类型: {uploaded_file.type}")
    
    # 读取文件内容
    bytes_data = uploaded_file.read()
    st.write(f"读取了 {len(bytes_data)} 字节")
    
    st.success("✅ 文件上传成功！")
