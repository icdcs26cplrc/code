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
    def __init__(self, encode_type: str, k_datablock: int, p_localgroup: int, r_globalparityblock: int, blocksize: int = 1048576):
        self.encode_type = encode_type  # 编码方案类型: azure_lrc, azure_lrc_1, optimal, uniform, new_lrc
        self.k_datablock = k_datablock  # 数据块数量
        self.p_localgroup = p_localgroup  # 本地组数量
        self.r_globalparityblock = r_globalparityblock  # 全局校验块数量
        self.blocksize = blocksize
        self.total_blocks = k_datablock + p_localgroup + r_globalparityblock
    
    def get_encoding_config(self) -> str:
        """获取编码配置字符串"""
        return f"{self.encode_type} {self.k_datablock} {self.p_localgroup} {self.r_globalparityblock} {self.blocksize}"
    
    def __str__(self):
        return f"{self.encode_type}({self.k_datablock},{self.p_localgroup},{self.r_globalparityblock})"

class TestFramework:
    def __init__(self):
        self.uploaded_files = []
        self.client_executable = "./client"
        self.test_data_dir = "test_data"
        
    def setup_test_environment(self):
        """设置测试环境"""
        os.makedirs(self.test_data_dir, exist_ok=True)
        
    def generate_test_data(self, size: int = None) -> str:
        """生成测试数据"""
        if size is None:
            size = random.randint(1024, 8192)
        return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + ' \n', k=size))
    
    def create_test_file(self, filename: str, data: str) -> str:
        """创建测试文件"""
        filepath = os.path.join(self.test_data_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(data)
        return filepath
    
    def execute_client_command(self, command: str, timeout: int = 60) -> Tuple[bool, str, float]:
        """执行客户端命令并返回成功状态、输出和执行时间"""
        start_time = time.time()
        try:
            process = subprocess.Popen(
                [self.client_executable],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout
            )
            
            stdout, stderr = process.communicate(input=command + '\n')
            execution_time = time.time() - start_time
            
            if process.returncode == 0:
                return True, stdout.strip(), execution_time
            else:
                return False, stderr.strip() if stderr else stdout.strip(), execution_time
                
        except subprocess.TimeoutExpired:
            process.kill()
            execution_time = time.time() - start_time
            return False, "Command timeout", execution_time
        except Exception as e:
            execution_time = time.time() - start_time
            return False, str(e), execution_time
    
    def input_encoding_scheme(self, strategy: EncodingStrategy) -> bool:
        """输入编码方案配置"""
        print(f"\n=== 配置编码方案 ===")
        print(f"编码配置: {strategy.get_encoding_config()}")
        
        # 这里假设需要通过某种方式配置编码方案
        # 如果有专门的配置命令，可以在这里执行
        success, output, _ = self.execute_client_command(strategy.get_encoding_config())
        
        if success:
            print(f"✓ 编码方案配置成功")
            return True
        else:
            print(f"✗ 编码方案配置失败: {output}")
            return False
    
    def perform_set_operations(self, strategy: EncodingStrategy, num_operations: int = 10) -> Dict:
        """执行多次SET操作"""
        print(f"\n=== 开始SET操作阶段 ===")
        print(f"将执行 {num_operations} 次SET操作")
        
        set_results = {
            'strategy': str(strategy),
            'operations': [],
            'success_count': 0,
            'total_operations': 0,
            'total_data_size': 0,
            'start_time': time.time()
        }
        
        for i in range(num_operations):
            print(f"\n--- SET操作 {i+1}/{num_operations} ---")
            
            # 生成测试数据
            data_size = random.randint(strategy.blocksize // 4, strategy.blocksize * 2)
            data = self.generate_test_data(data_size)
            filename = f"test_{strategy.encode_type}_set_{i+1:03d}.txt"
            filepath = self.create_test_file(filename, data)
            
            # 执行SET命令
            set_cmd = f"set {filepath}"
            print(f"执行命令: {set_cmd}")
            
            success, output, exec_time = self.execute_client_command(set_cmd)
            
            operation_result = {
                'operation_id': i + 1,
                'filename': filename,
                'filepath': filepath,
                'command': set_cmd,
                'success': success,
                'data_size': len(data),
                'execution_time': exec_time,
                'output': output[:300] if output else "",
                'timestamp': time.time()
            }
            
            set_results['operations'].append(operation_result)
            set_results['total_operations'] += 1
            set_results['total_data_size'] += len(data)
            
            if success:
                set_results['success_count'] += 1
                self.uploaded_files.append(filename)
                print(f"✓ SET操作成功")
                print(f"  文件: {filename}")
                print(f"  大小: {len(data)} bytes")
                print(f"  耗时: {exec_time:.2f}秒")
            else:
                print(f"✗ SET操作失败")
                print(f"  错误信息: {output[:200]}")
                print(f"  耗时: {exec_time:.2f}秒")
            
            # 短暂延迟避免系统过载
            time.sleep(1)
        
        set_results['end_time'] = time.time()
        set_results['duration'] = set_results['end_time'] - set_results['start_time']
        set_results['success_rate'] = set_results['success_count'] / set_results['total_operations'] if set_results['total_operations'] > 0 else 0
        
        print(f"\n=== SET操作阶段完成 ===")
        print(f"成功操作: {set_results['success_count']}/{set_results['total_operations']}")
        print(f"成功率: {set_results['success_rate']:.2%}")
        print(f"总数据量: {set_results['total_data_size']} bytes")
        print(f"总耗时: {set_results['duration']:.2f}秒")
        
        return set_results
    
    def perform_random_get_operations(self, num_operations: int = 20) -> Dict:
        """执行随机GET操作"""
        print(f"\n=== 开始随机GET操作阶段 ===")
        print(f"将执行 {num_operations} 次随机GET操作")
        
        get_results = {
            'operations': [],
            'success_count': 0,
            'total_operations': 0,
            'total_execution_time': 0,
            'start_time': time.time()
        }
        
        if not self.uploaded_files:
            print("警告: 没有可用的文件进行GET操作")
            return get_results
        
        print(f"可用文件数量: {len(self.uploaded_files)}")
        
        for i in range(num_operations):
            print(f"\n--- GET操作 {i+1}/{num_operations} ---")
            
            # 随机选择一个已上传的文件
            filename = random.choice(self.uploaded_files)
            get_cmd = f"get {filename}"
            print(f"执行命令: {get_cmd}")
            
            success, output, exec_time = self.execute_client_command(get_cmd)
            
            operation_result = {
                'operation_id': i + 1,
                'filename': filename,
                'command': get_cmd,
                'success': success,
                'execution_time': exec_time,
                'output': output[:300] if output else "",
                'timestamp': time.time()
            }
            
            get_results['operations'].append(operation_result)
            get_results['total_operations'] += 1
            get_results['total_execution_time'] += exec_time
            
            if success:
                get_results['success_count'] += 1
                print(f"✓ GET操作成功")
                print(f"  文件: {filename}")
                print(f"  耗时: {exec_time:.2f}秒")
            else:
                print(f"✗ GET操作失败")
                print(f"  文件: {filename}")
                print(f"  错误信息: {output[:200]}")
                print(f"  耗时: {exec_time:.2f}秒")
            
            # 短暂延迟
            time.sleep(0.5)
        
        get_results['end_time'] = time.time()
        get_results['duration'] = get_results['end_time'] - get_results['start_time']
        get_results['success_rate'] = get_results['success_count'] / get_results['total_operations'] if get_results['total_operations'] > 0 else 0
        get_results['average_execution_time'] = get_results['total_execution_time'] / get_results['total_operations'] if get_results['total_operations'] > 0 else 0
        
        print(f"\n=== 随机GET操作阶段完成 ===")
        print(f"成功操作: {get_results['success_count']}/{get_results['total_operations']}")
        print(f"成功率: {get_results['success_rate']:.2%}")
        print(f"平均执行时间: {get_results['average_execution_time']:.2f}秒")
        print(f"总耗时: {get_results['duration']:.2f}秒")
        
        return get_results
    
    def perform_random_repair_operations(self, strategy: EncodingStrategy, num_operations: int = 10) -> Dict:
        """执行随机REPAIR操作"""
        print(f"\n=== 开始随机REPAIR操作阶段 ===")
        print(f"将执行 {num_operations} 次随机REPAIR操作")
        
        repair_results = {
            'strategy': str(strategy),
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
        
        print(f"可修复的块: {all_blocks}")
        
        for i in range(num_operations):
            print(f"\n--- REPAIR操作 {i+1}/{num_operations} ---")
            
            # 随机选择要修复的块
            target_block = random.choice(all_blocks)
            repair_cmd = f"repair {target_block}"
            print(f"执行命令: {repair_cmd}")
            
            # 记录修复开始时间并执行命令
            success, output, exec_time = self.execute_client_command(repair_cmd)
            
            # 判断修复是否成功
            # 根据描述，"successfully repair"表示成功，"repair fail"表示失败
            # "finish send data"也视为成功
            repair_success = False
            if success:
                output_lower = output.lower()
                if "successfully repair" in output_lower or "finish send data" in output_lower:
                    repair_success = True
                elif "repair fail" in output_lower:
                    repair_success = False
                else:
                    # 如果命令执行成功但没有明确的成功/失败标识，默认为成功
                    repair_success = True
            
            operation_result = {
                'operation_id': i + 1,
                'target_block': target_block,
                'command': repair_cmd,
                'success': repair_success,
                'execution_time': exec_time,
                'repair_time': exec_time,  # 将执行时间作为修复时间
                'output': output[:300] if output else "",
                'timestamp': time.time()
            }
            
            repair_results['operations'].append(operation_result)
            repair_results['total_operations'] += 1
            
            if repair_success:
                repair_results['success_count'] += 1
                repair_results['total_repair_time'] += exec_time
                repair_results['successful_repair_times'].append(exec_time)
                print(f"✓ REPAIR操作成功")
                print(f"  目标块: {target_block}")
                print(f"  修复时间: {exec_time:.2f}秒")
            else:
                print(f"✗ REPAIR操作失败")
                print(f"  目标块: {target_block}")
                print(f"  错误信息: {output[:200]}")
                print(f"  执行时间: {exec_time:.2f}秒")
            
            # 短暂延迟
            time.sleep(1)
        
        repair_results['end_time'] = time.time()
        repair_results['duration'] = repair_results['end_time'] - repair_results['start_time']
        repair_results['success_rate'] = repair_results['success_count'] / repair_results['total_operations'] if repair_results['total_operations'] > 0 else 0
        repair_results['average_repair_time'] = repair_results['total_repair_time'] / repair_results['success_count'] if repair_results['success_count'] > 0 else 0
        
        # 计算修复时间统计
        if repair_results['successful_repair_times']:
            repair_results['min_repair_time'] = min(repair_results['successful_repair_times'])
            repair_results['max_repair_time'] = max(repair_results['successful_repair_times'])
            repair_results['median_repair_time'] = sorted(repair_results['successful_repair_times'])[len(repair_results['successful_repair_times'])//2]
        else:
            repair_results['min_repair_time'] = 0
            repair_results['max_repair_time'] = 0
            repair_results['median_repair_time'] = 0
        
        print(f"\n=== 随机REPAIR操作阶段完成 ===")
        print(f"成功操作: {repair_results['success_count']}/{repair_results['total_operations']}")
        print(f"成功率: {repair_results['success_rate']:.2%}")
        print(f"平均修复时间: {repair_results['average_repair_time']:.2f}秒")
        print(f"最短修复时间: {repair_results['min_repair_time']:.2f}秒")
        print(f"最长修复时间: {repair_results['max_repair_time']:.2f}秒")
        print(f"中位修复时间: {repair_results['median_repair_time']:.2f}秒")
        print(f"总耗时: {repair_results['duration']:.2f}秒")
        
        return repair_results
    
    def run_comprehensive_test(self, strategy: EncodingStrategy, 
                             set_operations: int = 10, 
                             get_operations: int = 20, 
                             repair_operations: int = 10) -> Dict:
        """运行综合测试"""
        print(f"\n{'='*80}")
        print(f"开始测试编码策略: {strategy}")
        print(f"测试参数:")
        print(f"  SET操作数量: {set_operations}")
        print(f"  GET操作数量: {get_operations}")
        print(f"  REPAIR操作数量: {repair_operations}")
        print(f"{'='*80}")
        
        test_result = {
            'strategy': str(strategy),
            'strategy_config': strategy.get_encoding_config(),
            'test_parameters': {
                'set_operations': set_operations,
                'get_operations': get_operations,
                'repair_operations': repair_operations
            },
            'start_time': time.time(),
            'set_results': None,
            'get_results': None,
            'repair_results': None,
            'error': None
        }
        
        try:
            # 清空之前的文件列表
            self.uploaded_files.clear()
            
            # 等待系统初始化
            print("等待系统初始化...")
            time.sleep(3)
            
            # 阶段1: 执行SET操作
            test_result['set_results'] = self.perform_set_operations(strategy, set_operations)
            
            # 等待一段时间确保数据写入完成
            print("\n等待数据写入完成...")
            time.sleep(2)
            
            # 阶段2: 执行随机GET操作
            test_result['get_results'] = self.perform_random_get_operations(get_operations)
            
            # 等待一段时间
            print("\n等待系统稳定...")
            time.sleep(2)
            
            # 阶段3: 执行随机REPAIR操作
            test_result['repair_results'] = self.perform_random_repair_operations(strategy, repair_operations)
            
        except KeyboardInterrupt:
            print(f"\n用户中断测试")
            test_result['error'] = "用户中断"
        except Exception as e:
            print(f"\n测试过程中发生错误: {str(e)}")
            test_result['error'] = str(e)
        
        test_result['end_time'] = time.time()
        test_result['total_duration'] = test_result['end_time'] - test_result['start_time']
        
        # 打印测试总结
        self.print_comprehensive_summary(test_result)
        
        return test_result
    
    def print_comprehensive_summary(self, test_result: Dict):
        """打印综合测试总结"""
        print(f"\n{'='*80}")
        print(f"测试总结报告")
        print(f"{'='*80}")
        print(f"编码策略: {test_result['strategy']}")
        print(f"配置参数: {test_result['strategy_config']}")
        print(f"总测试时间: {test_result['total_duration']:.2f}秒")
        
        if test_result['error']:
            print(f"测试错误: {test_result['error']}")
        
        # SET操作总结
        if test_result['set_results']:
            set_res = test_result['set_results']
            print(f"\n--- SET操作总结 ---")
            print(f"成功率: {set_res['success_count']}/{set_res['total_operations']} ({set_res['success_rate']:.2%})")
            print(f"总数据量: {set_res['total_data_size']} bytes")
            print(f"耗时: {set_res['duration']:.2f}秒")
        
        # GET操作总结
        if test_result['get_results']:
            get_res = test_result['get_results']
            print(f"\n--- GET操作总结 ---")
            print(f"成功率: {get_res['success_count']}/{get_res['total_operations']} ({get_res['success_rate']:.2%})")
            print(f"平均执行时间: {get_res['average_execution_time']:.2f}秒")
            print(f"耗时: {get_res['duration']:.2f}秒")
        
        # REPAIR操作总结
        if test_result['repair_results']:
            repair_res = test_result['repair_results']
            print(f"\n--- REPAIR操作总结 ---")
            print(f"成功率: {repair_res['success_count']}/{repair_res['total_operations']} ({repair_res['success_rate']:.2%})")
            print(f"平均修复时间: {repair_res['average_repair_time']:.2f}秒")
            print(f"修复时间范围: {repair_res['min_repair_time']:.2f}s - {repair_res['max_repair_time']:.2f}s")
            print(f"中位修复时间: {repair_res['median_repair_time']:.2f}秒")
            print(f"耗时: {repair_res['duration']:.2f}秒")
        
        print(f"{'='*80}")
    
    def save_test_results(self, test_result: Dict, filename: str = None):
        """保存测试结果到JSON文件"""
        if filename is None:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"test_results_{test_result['strategy'].replace('(', '_').replace(')', '').replace(',', '_')}_{timestamp}.json"
        
        filepath = os.path.join(self.test_data_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(test_result, f, indent=2, ensure_ascii=False)
            print(f"\n测试结果已保存到: {filepath}")
        except Exception as e:
            print(f"\n保存测试结果失败: {e}")


def create_encoding_strategies() -> List[EncodingStrategy]:
    """创建不同的编码策略进行测试"""
    strategies = [
        # 示例策略配置
        EncodingStrategy("azure_lrc", 4, 2, 2, 4096),
        EncodingStrategy("new_lrc", 4, 2, 2, 1048576),
        EncodingStrategy("optimal", 6, 2, 2, 1048576),
        EncodingStrategy("uniform", 8, 2, 2, 4096),
        # 可以添加更多策略
    ]
    return strategies


def main():
    """主函数"""
    print("分布式存储系统测试框架")
    print("="*50)
    
    # 创建测试框架实例
    test_framework = TestFramework()
    test_framework.setup_test_environment()
    
    # 检查客户端可执行文件
    if not os.path.exists(test_framework.client_executable):
        print(f"错误: 找不到客户端可执行文件 {test_framework.client_executable}")
        print("请确保客户端程序已编译并位于当前目录")
        return
    
    # 获取编码策略
    strategies = create_encoding_strategies()
    
    # 用户选择测试模式
    print("\n请选择测试模式:")
    print("1. 测试单个策略")
    print("2. 测试所有策略")
    print("3. 自定义策略测试")
    
    try:
        choice = input("请输入选择 (1-3): ").strip()
        
        if choice == "1":
            # 显示可用策略
            print("\n可用编码策略:")
            for i, strategy in enumerate(strategies):
                print(f"{i+1}. {strategy}")
            
            strategy_idx = int(input(f"请选择策略 (1-{len(strategies)}): ")) - 1
            if 0 <= strategy_idx < len(strategies):
                selected_strategy = strategies[strategy_idx]
                
                # 获取测试参数
                set_ops = int(input("SET操作数量 (默认10): ") or "10")
                get_ops = int(input("GET操作数量 (默认20): ") or "20")
                repair_ops = int(input("REPAIR操作数量 (默认10): ") or "10")
                
                # 运行测试
                result = test_framework.run_comprehensive_test(selected_strategy, set_ops, get_ops, repair_ops)
                test_framework.save_test_results(result)
            else:
                print("无效的策略选择")
        
        elif choice == "2":
            # 测试所有策略
            print(f"\n将测试 {len(strategies)} 个策略")
            all_results = []
            
            for i, strategy in enumerate(strategies):
                print(f"\n开始测试策略 {i+1}/{len(strategies)}: {strategy}")
                result = test_framework.run_comprehensive_test(strategy)
                all_results.append(result)
                test_framework.save_test_results(result)
                
                if i < len(strategies) - 1:
                    print("\n等待下一个测试...")
                    time.sleep(5)
            
            print(f"\n所有策略测试完成，共测试了 {len(all_results)} 个策略")
        
        elif choice == "3":
            # 自定义策略
            print("\n自定义编码策略:")
            print("支持的编码类型: azure_lrc, azure_lrc_1, optimal, uniform, new_lrc")
            
            encode_type = input("编码类型: ").strip()
            k_datablock = int(input("数据块数量 (k): "))
            p_localgroup = int(input("本地组数量 (p): "))
            r_globalparityblock = int(input("全局校验块数量 (r): "))
            blocksize = int(input("块大小 (默认1048576): ") or "1048576")
            
            custom_strategy = EncodingStrategy(encode_type, k_datablock, p_localgroup, r_globalparityblock, blocksize)
            
            # 获取测试参数
            set_ops = int(input("SET操作数量 (默认10): ") or "10")
            get_ops = int(input("GET操作数量 (默认20): ") or "20")
            repair_ops = int(input("REPAIR操作数量 (默认10): ") or "10")
            
            # 运行测试
            result = test_framework.run_comprehensive_test(custom_strategy, set_ops, get_ops, repair_ops)
            test_framework.save_test_results(result)
        
        else:
            print("无效的选择")
    
    except KeyboardInterrupt:
        print("\n\n测试被用户中断")
    except Exception as e:
        print(f"\n发生错误: {e}")
    
    print("\n测试程序结束")


if __name__ == "__main__":
    main()
