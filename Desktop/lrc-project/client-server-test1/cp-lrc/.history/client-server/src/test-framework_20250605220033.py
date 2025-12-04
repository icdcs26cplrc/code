import os
import sys
import time
import random
import string
import subprocess
import threading
import signal
import json
from pathlib import Path
from typing import List, Dict, Tuple

class EncodingStrategy:
    """编码策略类"""
    def __init__(self, name: str, k: int, p: int, r: int, block_size: int = 4096):
        self.name = name  # 编码方案名称 (azure_lrc, azure_lrc_1, optimal, uniform, new_lrc)
        self.k = k        # 数据块数量 (k_datablock)
        self.p = p        # 本地组数量 (p_localgroup)
        self.r = r        # 全局校验块数量 (r_gobalparityblock)
        self.block_size = block_size
        self.total_blocks = k + p + r
    
    def get_coordinator_config(self) -> str:
        """获取coordinator配置字符串"""
        return f"{self.name} {self.k} {self.p} {self.r} {self.block_size}"
    
    def get_datanode_config(self) -> str:
        """获取datanode配置字符串"""
        return f"{self.total_blocks} {self.r} {self.p} {self.block_size}"
    
    def __str__(self):
        return f"{self.name}({self.k},{self.p},{self.r})"

class TestFramework:
    def __init__(self):
        self.uploaded_files = []
        self.client_executable = "./client"
        self.test_results = {}
        
    def generate_test_data(self, size: int = None) -> str:
        """生成测试数据"""
        if size is None:
            size = random.randint(1024, 8192)
        return ''.join(random.choices(string.ascii_letters + string.digits, k=size))
    
    def create_test_file(self, filename: str, data: str) -> str:
        """创建测试文件"""
        filepath = os.path.join("test_data", filename)
        os.makedirs("test_data", exist_ok=True)
        with open(filepath, 'w') as f:
            f.write(data)
        return filepath
    
    def execute_client_command(self, command: str) -> Tuple[bool, str, float]:
        """执行客户端命令，返回成功状态、输出和执行时间"""
        try:
            start_time = time.time()
            process = subprocess.Popen(
                [self.client_executable],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=60
            )
            
            stdout, stderr = process.communicate(input=command + '\n')
            end_time = time.time()
            execution_time = end_time - start_time
            
            if process.returncode == 0:
                return True, stdout.strip(), execution_time
            else:
                return False, stderr.strip(), execution_time
                
        except subprocess.TimeoutExpired:
            process.kill()
            return False, "Command timeout", 60.0
        except Exception as e:
            return False, str(e), 0.0
    
    def perform_set_operations(self, strategy: EncodingStrategy, num_operations: int = 10) -> Dict:
        """执行多次SET操作"""
        print(f"\n{'='*50}")
        print(f"开始SET操作测试 - 策略: {strategy}")
        print(f"{'='*50}")
        
        set_results = {
            'strategy': str(strategy),
            'operations': [],
            'success_count': 0,
            'total_operations': 0,
            'total_time': 0,
            'start_time': time.time()
        }
        
        for i in range(num_operations):
            print(f"\nSET操作 {i+1}/{num_operations}")
            
            # 生成测试数据
            data = self.generate_test_data()
            filename = f"test_{strategy.name}_set_{i}.txt"
            filepath = self.create_test_file(filename, data)
            
            # 执行SET命令
            set_cmd = f"set {filepath}"
            success, output, exec_time = self.execute_client_command(set_cmd)
            
            operation_result = {
                'operation_id': i,
                'filename': filename,
                'filepath': filepath,
                'command': set_cmd,
                'success': success,
                'data_size': len(data),
                'execution_time': exec_time,
                'output': output[:300],
                'timestamp': time.time()
            }
            
            set_results['operations'].append(operation_result)
            set_results['total_operations'] += 1
            set_results['total_time'] += exec_time
            
            if success:
                set_results['success_count'] += 1
                self.uploaded_files.append(filename)
                print(f"✓ SET操作成功 - 文件: {filename}")
                print(f"  数据大小: {len(data)} bytes")
                print(f"  执行时间: {exec_time:.3f}秒")
            else:
                print(f"✗ SET操作失败 - 文件: {filename}")
                print(f"  错误信息: {output[:100]}")
            
            time.sleep(1)  # 短暂延迟避免系统过载
        
        set_results['end_time'] = time.time()
        set_results['duration'] = set_results['end_time'] - set_results['start_time']
        set_results['success_rate'] = set_results['success_count'] / set_results['total_operations'] if set_results['total_operations'] > 0 else 0
        set_results['average_time'] = set_results['total_time'] / set_results['total_operations'] if set_results['total_operations'] > 0 else 0
        
        print(f"\n{'='*30} SET操作总结 {'='*30}")
        print(f"成功操作: {set_results['success_count']}/{set_results['total_operations']}")
        print(f"成功率: {set_results['success_rate']:.2%}")
        print(f"平均执行时间: {set_results['average_time']:.3f}秒")
        print(f"总耗时: {set_results['duration']:.2f}秒")
        
        return set_results
    
    def perform_random_get_operations(self, num_operations: int = 20) -> Dict:
        """执行随机GET操作"""
        print(f"\n{'='*50}")
        print(f"开始随机GET操作测试")
        print(f"{'='*50}")
        
        get_results = {
            'operations': [],
            'success_count': 0,
            'total_operations': 0,
            'total_time': 0,
            'start_time': time.time()
        }
        
        if not self.uploaded_files:
            print("警告: 没有可用的文件进行GET操作")
            return get_results
        
        for i in range(num_operations):
            print(f"\nGET操作 {i+1}/{num_operations}")
            
            # 随机选择一个已上传的文件
            filename = random.choice(self.uploaded_files)
            get_cmd = f"get {filename}"
            
            success, output, exec_time = self.execute_client_command(get_cmd)
            
            operation_result = {
                'operation_id': i,
                'filename': filename,
                'command': get_cmd,
                'success': success,
                'execution_time': exec_time,
                'output': output[:300],
                'timestamp': time.time()
            }
            
            get_results['operations'].append(operation_result)
            get_results['total_operations'] += 1
            get_results['total_time'] += exec_time
            
            if success:
                get_results['success_count'] += 1
                print(f"✓ GET操作成功 - 文件: {filename}")
                print(f"  执行时间: {exec_time:.3f}秒")
            else:
                print(f"✗ GET操作失败 - 文件: {filename}")
                print(f"  错误信息: {output[:100]}")
            
            time.sleep(1)  # 短暂延迟
        
        get_results['end_time'] = time.time()
        get_results['duration'] = get_results['end_time'] - get_results['start_time']
        get_results['success_rate'] = get_results['success_count'] / get_results['total_operations'] if get_results['total_operations'] > 0 else 0
        get_results['average_time'] = get_results['total_time'] / get_results['total_operations'] if get_results['total_operations'] > 0 else 0
        
        print(f"\n{'='*30} GET操作总结 {'='*30}")
        print(f"成功操作: {get_results['success_count']}/{get_results['total_operations']}")
        print(f"成功率: {get_results['success_rate']:.2%}")
        print(f"平均执行时间: {get_results['average_time']:.3f}秒")
        print(f"总耗时: {get_results['duration']:.2f}秒")
        
        return get_results
    
    def perform_random_repair_operations(self, strategy: EncodingStrategy, num_operations: int = 10) -> Dict:
        """执行随机REPAIR操作"""
        print(f"\n{'='*50}")
        print(f"开始随机REPAIR操作测试")
        print(f"{'='*50}")
        
        repair_results = {
            'operations': [],
            'success_count': 0,
            'total_operations': 0,
            'total_repair_time': 0,
            'successful_repair_times': [],
            'start_time': time.time()
        }
        
        # 生成所有可能的块标识符
        all_blocks = []
        for block_id in range(strategy.total_blocks):
            all_blocks.append(f"stripe_0_block_{block_id}")
        
        print(f"可用块列表: {all_blocks}")
        
        for i in range(num_operations):
            print(f"\nREPAIR操作 {i+1}/{num_operations}")
            
            # 随机选择要修复的块
            target_block = random.choice(all_blocks)
            repair_cmd = f"repair {target_block}"
            
            print(f"执行命令: {repair_cmd}")
            success, output, exec_time = self.execute_client_command(repair_cmd)
            
            # 判断修复是否成功
            # 根据描述，检查输出中是否包含成功或失败的关键字
            repair_success = False
            if success:
                output_lower = output.lower()
                if "successfully repair" in output_lower or "finish send data" in output_lower:
                    repair_success = True
                elif "repair fail" in output_lower:
                    repair_success = False
                else:
                    # 如果没有明确的失败信息，且命令执行成功，则认为修复成功
                    repair_success = True
            
            operation_result = {
                'operation_id': i,
                'target_block': target_block,
                'command': repair_cmd,
                'success': repair_success,
                'execution_time': exec_time,
                'output': output[:300],
                'timestamp': time.time()
            }
            
            repair_results['operations'].append(operation_result)
            repair_results['total_operations'] += 1
            
            if repair_success:
                repair_results['success_count'] += 1
                repair_results['total_repair_time'] += exec_time
                repair_results['successful_repair_times'].append(exec_time)
                print(f"✓ REPAIR操作成功 - 块: {target_block}")
                print(f"  修复时间: {exec_time:.3f}秒")
            else:
                print(f"✗ REPAIR操作失败 - 块: {target_block}")
                print(f"  错误信息: {output[:100]}")
            
            time.sleep(1)  # 短暂延迟
        
        repair_results['end_time'] = time.time()
        repair_results['duration'] = repair_results['end_time'] - repair_results['start_time']
        repair_results['success_rate'] = repair_results['success_count'] / repair_results['total_operations'] if repair_results['total_operations'] > 0 else 0
        repair_results['average_repair_time'] = repair_results['total_repair_time'] / repair_results['success_count'] if repair_results['success_count'] > 0 else 0
        
        print(f"\n{'='*30} REPAIR操作总结 {'='*30}")
        print(f"成功操作: {repair_results['success_count']}/{repair_results['total_operations']}")
        print(f"成功率: {repair_results['success_rate']:.2%}")
        print(f"平均修复时间: {repair_results['average_repair_time']:.3f}秒")
        print(f"总耗时: {repair_results['duration']:.2f}秒")
        
        if repair_results['successful_repair_times']:
            min_time = min(repair_results['successful_repair_times'])
            max_time = max(repair_results['successful_repair_times'])
            print(f"最快修复时间: {min_time:.3f}秒")
            print(f"最慢修复时间: {max_time:.3f}秒")
        
        return repair_results
    
    def run_comprehensive_test(self, strategy: EncodingStrategy, 
                             set_operations: int = 10, 
                             get_operations: int = 20, 
                             repair_operations: int = 10) -> Dict:
        """运行综合测试"""
        print(f"\n{'='*80}")
        print(f"开始综合测试")
        print(f"编码策略: {strategy}")
        print(f"配置参数: {strategy.get_coordinator_config()}")
        print(f"SET操作数: {set_operations}, GET操作数: {get_operations}, REPAIR操作数: {repair_operations}")
        print(f"{'='*80}")
        
        test_result = {
            'strategy': str(strategy),
            'strategy_config': strategy.get_coordinator_config(),
            'start_time': time.time(),
            'set_results': None,
            'get_results': None,
            'repair_results': None,
            'test_parameters': {
                'set_operations': set_operations,
                'get_operations': get_operations,
                'repair_operations': repair_operations
            }
        }
        
        try:
            # 清空之前的文件列表
            self.uploaded_files.clear()
            
            # 等待系统初始化
            print("等待系统初始化...")
            time.sleep(3)
            
            # 1. 执行SET操作阶段
            test_result['set_results'] = self.perform_set_operations(strategy, set_operations)
            
            # 短暂等待
            time.sleep(2)
            
            # 2. 执行随机GET操作阶段
            test_result['get_results'] = self.perform_random_get_operations(get_operations)
            
            # 短暂等待
            time.sleep(2)
            
            # 3. 执行随机REPAIR操作阶段
            test_result['repair_results'] = self.perform_random_repair_operations(strategy, repair_operations)
            
        except KeyboardInterrupt:
            print("\n测试被用户中断")
        except Exception as e:
            print(f"\n测试过程中发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
        
        test_result['end_time'] = time.time()
        test_result['total_duration'] = test_result['end_time'] - test_result['start_time']
        
        # 保存测试结果
        self.test_results[str(strategy)] = test_result
        
        # 打印测试总结
        self.print_comprehensive_summary(test_result)
        
        return test_result
    
    def print_comprehensive_summary(self, test_result: Dict):
        """打印综合测试总结"""
        print(f"\n{'='*80}")
        print(f"综合测试总结 - {test_result['strategy']}")
        print(f"{'='*80}")
        
        print(f"测试配置: {test_result['strategy_config']}")
        print(f"总测试时间: {test_result['total_duration']:.2f}秒")
        
        # SET操作总结
        if test_result['set_results']:
            set_res = test_result['set_results']
            print(f"\nSET操作:")
            print(f"  成功率: {set_res['success_rate']:.2%} ({set_res['success_count']}/{set_res['total_operations']})")
            print(f"  平均时间: {set_res['average_time']:.3f}秒")
        
        # GET操作总结
        if test_result['get_results']:
            get_res = test_result['get_results']
            print(f"\nGET操作:")
            print(f"  成功率: {get_res['success_rate']:.2%} ({get_res['success_count']}/{get_res['total_operations']})")
            print(f"  平均时间: {get_res['average_time']:.3f}秒")
        
        # REPAIR操作总结
        if test_result['repair_results']:
            repair_res = test_result['repair_results']
            print(f"\nREPAIR操作:")
            print(f"  成功率: {repair_res['success_rate']:.2%} ({repair_res['success_count']}/{repair_res['total_operations']})")
            print(f"  平均修复时间: {repair_res['average_repair_time']:.3f}秒")
        
        print(f"\n{'='*80}")
    
    def save_results_to_file(self, filename: str = None):
        """保存测试结果到文件"""
        if filename is None:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"test_results_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.test_results, f, indent=2, ensure_ascii=False, default=str)
            print(f"测试结果已保存到: {filename}")
        except Exception as e:
            print(f"保存结果失败: {str(e)}")

def get_encoding_strategy_from_input():
    """从用户输入获取编码策略"""
    print("请输入编码方案配置:")
    print("格式: EncodeType k_datablock p_localgroup r_gobalparityblock blocksize")
    print("支持的EncodeType: azure_lrc, azure_lrc_1, optimal, uniform, new_lrc")
    print("示例: new_lrc 4 2 2 1048576")
    print("     azure 4 2 2 4096")
    
    while True:
        try:
            user_input = input("\n请输入配置: ").strip()
            if not user_input:
                continue
                
            parts = user_input.split()
            if len(parts) != 5:
                print("错误: 请输入5个参数")
                continue
            
            encode_type = parts[0]
            k = int(parts[1])
            p = int(parts[2])
            r = int(parts[3])
            block_size = int(parts[4])
            
            if encode_type not in ['azure_lrc', 'azure_lrc_1', 'optimal', 'uniform', 'new_lrc']:
                print(f"错误: 不支持的编码类型 {encode_type}")
                continue
            
            strategy = EncodingStrategy(encode_type, k, p, r, block_size)
            print(f"创建编码策略: {strategy}")
            return strategy
            
        except ValueError as e:
            print(f"错误: 参数格式不正确 - {str(e)}")
        except KeyboardInterrupt:
            print("\n程序退出")
            sys.exit(0)

def main():
    """主函数"""
    print("分布式存储系统测试框架")
    print("=" * 50)
    
    # 检查客户端可执行文件
    if not os.path.exists("./client"):
        print("错误: 找不到客户端可执行文件 './client'")
        print("请确保客户端程序存在并且有执行权限")
        sys.exit(1)
    
    # 创建测试框架
    framework = TestFramework()
    
    try:
        # 获取编码策略
        strategy = get_encoding_strategy_from_input()
        
        # 获取测试参数
        print("\n请输入测试参数:")
        try:
            set_ops = int(input("SET操作次数 (默认10): ") or "10")
            get_ops = int(input("GET操作次数 (默认20): ") or "20")
            repair_ops = int(input("REPAIR操作次数 (默认10): ") or "10")
        except ValueError:
            print("使用默认参数: SET=10, GET=20, REPAIR=10")
            set_ops, get_ops, repair_ops = 10, 20, 10
        
        # 运行测试
        result = framework.run_comprehensive_test(strategy, set_ops, get_ops, repair_ops)
        
        # 保存结果
        save_file = input("\n是否保存测试结果到文件? (y/n): ").strip().lower()
        if save_file in ['y', 'yes']:
            framework.save_results_to_file()
        
        print("\n测试完成!")
        
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n程序发生错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()