import os
import random
import string
from pathlib import Path

def generate_random_files(output_dir, file_count, min_size_kb, max_size_kb):
    """
    生成指定数量和大小范围的随机txt文件
    
    参数:
    output_dir: 输出目录路径
    file_count: 要生成的文件数量
    min_size_kb: 最小文件大小(KB)
    max_size_kb: 最大文件大小(KB)
    """
    
    # 确保输出目录存在
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 用于生成随机内容的字符集
    chars = string.ascii_letters + string.digits + string.punctuation + ' \n'
    
    print(f"开始生成 {file_count} 个文件到目录: {output_dir}")
    print(f"文件大小范围: {min_size_kb}KB - {max_size_kb}KB")
    print("-" * 50)
    
    for i in range(file_count):
        # 生成随机文件大小（字节）
        file_size_bytes = random.randint(min_size_kb * 1024, max_size_kb * 1024)
        
        # 生成文件名
        filename = f"random_file_{i+1:04d}.txt"
        filepath = os.path.join(output_dir, filename)
        
        # 生成随机内容
        content = ""
        current_size = 0
        
        while current_size < file_size_bytes:
            # 随机选择要添加的内容类型
            if random.random() < 0.1:  # 10%概率添加换行
                char = '\n'
            elif random.random() < 0.2:  # 20%概率添加空格
                char = ' '
            else:  # 70%概率添加其他字符
                char = random.choice(string.ascii_letters + string.digits)
            
            content += char
            current_size = len(content.encode('utf-8'))
        
        # 截断到精确大小
        while len(content.encode('utf-8')) > file_size_bytes:
            content = content[:-1]
        
        # 写入文件
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        actual_size = os.path.getsize(filepath)
        print(f"已生成: {filename} ({actual_size} bytes, {actual_size/1024:.2f} KB)")
    
    print(f"\n✅ 成功生成 {file_count} 个文件!")

def main():
    """主函数 - 交互式输入参数"""
    print("=== 随机文件生成器 ===")
    
    # 获取用户输入
    output_dir = input("请输入输出目录路径 (默认: ./random_files): ").strip()
    if not output_dir:
        output_dir = "./random_files"
    
    try:
        file_count = int(input("请输入要生成的文件数量: "))
        min_size_kb = int(input("请输入最小文件大小(KB): "))
        max_size_kb = int(input("请输入最大文件大小(KB): "))
        
        if file_count <= 0:
            raise ValueError("文件数量必须大于0")
        if min_size_kb <= 0 or max_size_kb <= 0:
            raise ValueError("文件大小必须大于0")
        if min_size_kb > max_size_kb:
            raise ValueError("最小文件大小不能大于最大文件大小")
        
        # 生成文件
        generate_random_files(output_dir, file_count, min_size_kb, max_size_kb)
        
    except ValueError as e:
        print(f"❌ 输入错误: {e}")
    except Exception as e:
        print(f"❌ 发生错误: {e}")

# 直接调用函数的示例
def example_usage():
    """使用示例"""
    # 在当前目录下的test_files文件夹中生成5个文件
    # 文件大小在1KB到10KB之间
    generate_random_files(
        output_dir="./test_files", 
        file_count=5, 
        min_size_kb=1, 
        max_size_kb=10
    )

if __name__ == "__main__":
    # 选择运行方式
    choice = input("选择运行方式 (1: 交互式输入, 2: 运行示例): ").strip()
    
    if choice == "1":
        main()
    elif choice == "2":
        example_usage()
    else:
        print("无效选择，运行交互式输入模式")
        main()
