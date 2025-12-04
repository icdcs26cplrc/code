#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分布式存储系统测试框架
支持多种编码策略的SET、GET、REPAIR操作测试
"""

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
import argparse

class EncodingStrategy:
    """编码策略类"""
    def __init__(self, name: str, k: int, p: int, r: int, block_size: int = 4096):
        self.name = name  # 编码方案名称
        self.k = k        # 数据块数量 (k_datablock)
        self.p = p        # 本地组数量 (p_localgroup)
        self.r = r        # 全局校验块数量 (r_globalparityblock)
        self.block_size = block_size
        self.total_blocks = k + p + r
    
    def get_coordinator_config(self) -> str:
        """获取coordinator配置字符串"""
        return f"{self.name} {self.k} {self.p} {self.r} {self.block_size}"
    
    def get_datanode_config(self) -> str:
        """获取datanode配置字符串"""
        return f"{self.total_blocks} {self.r} {self.p} {self.block_size}"
    
    def __str__(self):
        return f"{self.name}(k={self.k},p={self.p},r={self.r},block_size={self.block_size})"

class TestFramework:
    def __init__(self, client_path: str = "./client"):
        self.uploaded_files = []
        self.client_executable = client_path
        self.test_data_dir = "test_data"
        
        # 创建测试数据目录
        os.makedirs(self.test_data_dir, exist_ok=True)
    
    def generate_test_data(self, size: int = None) -> str:
        """生成测试数据"""
        if size is None:
            size = random.randint(1024, 8192)
        return ''.join(random.choices(string.ascii_letters + string.digits + '\n', k=size))
    
    def create_test_file(self, filename: str, data: str) -> str:
        """创建测试文件"""
        filepath = os.path.join(self.test_data_dir, filename)
        with open(filepath, 'w') as f:
            f.write(data)
        return filepath
    
    def execute_client_command(self, command: str, timeout: int = 30) -> Tuple[bool, str, float]:
        """执行客户端命令并返回结果和执行时间"""
        try:
            start_time = time.time()
            process = subprocess.Popen(
                [self.client_executable],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout
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
            return False, "Command timeout", timeout
        except Exception as e:
            return False, str(e), 0
    
    def perform_set_operations(self, strategy: EncodingStrategy, num_operations: int = 10) -> Dict:
        """执行多次SET操作"""
        print(f"\n{'='*60}")
        print(f"开始SET操作测试 - 策略: {strategy}")
        print(f"{'='*60}")
        
        set_results = {
            'strategy': str(strategy),
            'operations': [],
            'success_count': 0,
            'total_operations': num_operations,
            'start_time': time.time(),
            'total_data_size': 0
        }
        
        for i in range(num_operations):
            print(f"\n--- SET操作 {i+1}/{num_operations} ---")
            
            # 生成测试数据
            data_size = random.randint(2048, 16384)  # 随机数据大小
            data = self.generate_test_data(data_size)
            filename = f"test_{strategy.name}_set_{i}_{int(time.time())}.txt"
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
                'output': output[:300],  # 限制输出长度
                'timestamp': time.time()
            }
            
            set_results['operations'].append(operation_result)
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
                print(f"  文件: {filename}")
                print(f"  错误: {output[:150]}")
            
            # 操作间隔
            time.sleep(1)
        
        set_results['end_time'] = time.time()
        set_results['duration'] = set_results['end_time'] - set_results['start_time']
        set_results['success_rate'] = set_results['success_count'] / set_results['total_operations']
        
        print(f"\n{'='*50}")
        print(f"SET操作测试完成:")
        print(f"  成功: {set_results['success_count']}/{set_results['total_operations']}")
        print(f"  成功率: {set_results['success_rate']:.2%}")
        print(f"  总数据量: {set_results['total_data_size']} bytes")
        print(f"  总耗时: {set_results['duration']:.2f}秒")
        print(f"{'='*50}")
        
        return set_results
    
    def perform_random_get_operations(self, num_operations: int = 20) -> Dict:
        """执行随机GET操作"""
        print(f"\n{'='*60}")
        print(f"开始随机GET操作测试")
        print(f"{'='*60}")
        
        get_results = {
            'operations': [],
            'success_count': 0,
            'total_operations': num_operations,
            'start_time': time.time()
        }
        
        if not self.uploaded_files:
            print("⚠️  没有可用的文件进行GET操作")
            return get_results
        
        for i in range(num_operations):
            print(f"\n--- GET操作 {i+1}/{num_operations} ---")
            
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
            
            if success:
                get_results['success_count'] += 1
                print(f"✓ GET操作成功")
                print(f"  文件: {filename}")
                print(f"  耗时: {exec_time:.2f}秒")
            else:
                print(f"✗ GET操作失败")
                print(f"  文件: {filename}")
                print(f"  错误: {output[:150]}")
            
            # 操作间隔
            time.sleep(1)
        
        get_results['end_time'] = time.time()
        get_results['duration'] = get_results['end_time'] - get_results['start_time']
        get_results['success_rate'] = get_results['success_count'] / get_results['total_operations']
        get_results['average_response_time'] = sum(op['execution_time'] for op in get_results['operations'] if op['success']) / get_results['success_count'] if get_results['success_count'] > 0 else 0
        
        print(f"\n{'='*50}")
        print(f"GET操作测试完成:")
        print(f"  成功: {get_results['success_count']}/{get_results['total_operations']}")
        print(f"  成功率: {get_results['success_rate']:.2%}")
        print(f"  平均响应时间: {get_results['average_response_time']:.2f}秒")
        print(f"  总耗时: {get_results['duration']:.2f}秒")
        print(f"{'='*50}")
        
        return get_results
    
    def perform_random_repair_operations(self, strategy: EncodingStrategy, num_operations: int = 10) -> Dict:
        """执行随机REPAIR操作"""
        print(f"\n{'='*60}")
        print(f"开始随机REPAIR操作测试 - 策略: {strategy}")
        print(f"{'='*60}")
        
        repair_results = {
            'strategy': str(strategy),
            'operations': [],
            'success_count': 0,
            'total_operations': num_operations,
            'total_repair_time': 0,
            'start_time': time.time(),
            'repair_times': []
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
            
            print(f"修复目标: {target_block}")
            
            # 执行REPAIR命令并记录时间
            success, output, repair_time = self.execute_client_command(repair_cmd, timeout=60)
            
            # 判断修复是否成功
            # 根据输出判断成功与否
            repair_success = False
            if success:
                output_lower = output.lower()
                if "successfully repair" in output_lower or "finish send data" in output_lower:
                    repair_success = True
                elif "repair fail" in output_lower or "error" in output_lower:
                    repair_success = False
                else:
                    # 如果没有明确的失败信息，且命令执行成功，则认为修复成功
                    repair_success = True
            
            operation_result = {
                'operation_id': i,
                'target_block': target_block,
                'command': repair_cmd,
                'success': repair_success,
                'repair_time': repair_time,
                'output': output[:300],
                'timestamp': time.time()
            }
            
            repair_results['operations'].append(operation_result)
            
            if repair_success:
                repair_results['success_count'] += 1
                repair_results['total_repair_time'] += repair_time
                repair_results['repair_times'].append(repair_time)
                print(f"✓ REPAIR操作成功")
                print(f"  修复时间: {repair_time:.2f}秒")
            else:
                print(f"✗ REPAIR操作失败")
                print(f"  错误信息: {output[:150]}")
            
            # 操作间隔
            time.sleep(2)
        
        repair_results['end_time'] = time.time()
        repair_results['duration'] = repair_results['end_time'] - repair_results['start_time']
        repair_results['success_rate'] = repair_results['success_count'] / repair_results['total_operations']
        repair_results['average_repair_time'] = repair_results['total_repair_time'] / repair_results['success_count'] if repair_results['success_count'] > 0 else 0
        
        # 计算修复时间统计
        if repair_results['repair_times']:
            repair_results['min_repair_time'] = min(repair_results['repair_times'])
            repair_results['max_repair_time'] = max(repair_results['repair_times'])
            repair_results['median_repair_time'] = sorted(repair_results['repair_times'])[len(repair_results['repair_times'])//2]
        
        print(f"\n{'='*50}")
        print(f"REPAIR操作测试完成:")
        print(f"  成功: {repair_results['success_count']}/{repair_results['total_operations']}")
        print(f"  成功率: {repair_results['success_rate']:.2%}")
        print(f"  平均修复时间: {repair_results['average_repair_time']:.2f}秒")
        if repair_results['repair_times']:
            print(f"  最快修复时间: {repair_results['min_repair_time']:.2f}秒")
            print(f"  最慢修复时间: {repair_results['max_repair_time']:.2f}秒")
            print(f"  中位修复时间: {repair_results['median_repair_time']:.2f}秒")
        print(f"  总耗时: {repair_results['duration']:.2f}秒")
        print(f"{'='*50}")
        
        return repair_results
    
    def run_comprehensive_test(self, strategy: EncodingStrategy, 
                             set_operations: int = 10, 
                             get_operations: int = 20, 
                             repair_operations: int = 10) -> Dict:
        """运行综合测试"""
        print(f"\n{'='*80}")
        print(f"开始综合测试")
        print(f"编码策略: {strategy}")
        print(f"SET操作数: {set_operations}, GET操作数: {get_operations}, REPAIR操作数: {repair_operations}")
        print(f"{'='*80}")
        
        test_result = {
            'strategy': str(strategy),
            'config': {
                'set_operations': set_operations,
                'get_operations': get_operations,
                'repair_operations': repair_operations
            },
            'start_time': time.time(),
            'set_results': None,
            'get_results': None,
            'repair_results': None,
            'overall_success': False
        }
        
        try:
            # 清空之前的文件列表
            self.uploaded_files.clear()
            
            # 等待系统初始化
            print("⏳ 等待系统初始化...")
            time.sleep(3)
            
            # 1. 执行SET操作（按顺序）
            test_result['set_results'] = self.perform_set_operations(strategy, set_operations)
            
            # 等待一段时间确保SET操作完全完成
            time.sleep(2)
            
            # 2. 执行随机GET操作
            test_result['get_results'] = self.perform_random_get_operations(get_operations)
            
            # 等待一段时间
            time.sleep(2)
            
            # 3. 执行随机REPAIR操作
            test_result['repair_results'] = self.perform_random_repair_operations(strategy, repair_operations)
            
            test_result['overall_success'] = True
            
        except Exception as e:
            print(f"❌ 测试过程中发生错误: {str(e)}")
            test_result['error'] = str(e)
        
        test_result['end_time'] = time.time()
        test_result['total_duration'] = test_result['end_time'] - test_result['start_time']
        
        # 打印测试总结
        self.print_test_summary(test_result)
        
        return test_result
    
    def print_test_summary(self, test_result: Dict):
        """打印测试总结"""
        print(f"\n{'='*80}")
        print(f"测试总结报告")
        print(f"{'='*80}")
        print(f"编码策略: {test_result['strategy']}")
        print(f"总测试时间: {test_result['total_duration']:.2f}秒")
        print(f"测试状态: {'✓ 成功' if test_result['overall_success'] else '✗ 失败'}")
        
        if test_result['set_results']:
            set_res = test_result['set_results']
            print(f"\n📤 SET操作:")
            print(f"  成功率: {set_res['success_rate']:.2%} ({set_res['success_count']}/{set_res['total_operations']})")
            print(f"  总数据量: {set_res['total_data_size']} bytes")
            print(f"  耗时: {set_res['duration']:.2f}秒")
        
        if test_result['get_results']:
            get_res = test_result['get_results']
            print(f"\n📥 GET操作:")
            print(f"  成功率: {get_res['success_rate']:.2%} ({get_res['success_count']}/{get_res['total_operations']})")
            print(f"  平均响应时间: {get_res.get('average_response_time', 0):.2f}秒")
            print(f"  耗时: {get_res['duration']:.2f}秒")
        
        if test_result['repair_results']:
            repair_res = test_result['repair_results']
            print(f"\n🔧 REPAIR操作:")
            print(f"  成功率: {repair_res['success_rate']:.2%} ({repair_res['success_count']}/{repair_res['total_operations']})")
            print(f"  平均修复时间: {repair_res['average_repair_time']:.2f}秒")
            if 'min_repair_time' in repair_res:
                print(f"  修复时间范围: {repair_res['min_repair_time']:.2f}秒 - {repair_res['max_repair_time']:.2f}秒")
            print(f"  耗时: {repair_res['duration']:.2f}秒")
        
        print(f"{'='*80}")
    
    def save_results_to_file(self, test_result: Dict, filename: str = None):
        """保存测试结果到文件"""
        if filename is None:
            timestamp = int(time.time())
            filename = f"test_results_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(test_result, f, indent=2, ensure_ascii=False)
            print(f"📄 测试结果已保存到: {filename}")
        except Exception as e:
            print(f"❌ 保存结果失败: {str(e)}")

def create_encoding_strategies() -> List[EncodingStrategy]:
    """创建预定义的编码策略"""
    strategies = [
        EncodingStrategy("azure_lrc", 4, 2, 2, 4096),
        EncodingStrategy("azure_lrc_1", 6, 2, 2, 4096),
        EncodingStrategy("optimal", 4, 2, 2, 8192),
        EncodingStrategy("uniform", 3, 1, 2, 4096),
        EncodingStrategy("new_lrc", 4, 2, 2, 1048576),
    ]
    return strategies

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='分布式存储系统测试框架')
    parser.add_argument('--client', default='./client', help='客户端可执行文件路径')
    parser.add_argument('--strategy', help='编码策略名称')
    parser.add_argument('--k', type=int, help='数据块数量')
    parser.add_argument('--p', type=int, help='本地组数量')
    parser.add_argument('--r', type=int, help='全局校验块数量')
    parser.add_argument('--block-size', type=int, default=4096, help='块大小')
    parser.add_argument('--set-ops', type=int, default=10, help='SET操作次数')
    parser.add_argument('--get-ops', type=int, default=20, help='GET操作次数')
    parser.add_argument('--repair-ops', type=int, default=10, help='REPAIR操作次数')
    parser.add_argument('--save-results', action='store_true', help='保存测试结果到文件')
    
    args = parser.parse_args()
    
    # 创建测试框架
    test_framework = TestFramework(args.client)
    
    # 确定编码策略
    if args.strategy and args.k and args.p and args.r:
        # 使用命令行参数指定的策略
        strategy = EncodingStrategy(args.strategy, args.k, args.p, args.r, args.block_size)
        strategies = [strategy]
    else:
        # 使用预定义策略
        strategies = create_encoding_strategies()
        print("使用预定义编码策略:")
        for i, strategy in enumerate(strategies):
            print(f"  {i+1}. {strategy}")
        
        choice = input(f"\n请选择编码策略 (1-{len(strategies)}) 或按回车测试所有策略: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(strategies):
            strategies = [strategies[int(choice) - 1]]
    
    # 执行测试
    all_results = []
    
    for strategy in strategies:
        print(f"\n🚀 开始测试策略: {strategy}")
        
        result = test_framework.run_comprehensive_test(
            strategy, 
            args.set_ops, 
            args.get_ops, 
            args.repair_ops
        )
        
        all_results.append(result)
        
        if args.save_results:
            filename = f"test_{strategy.name}_k{strategy.k}_p{strategy.p}_r{strategy.r}_{int(time.time())}.json"
            test_framework.save_results_to_file(result, filename)
        
        # 策略间间隔
        if len(strategies) > 1:
            print("\n⏳ 等待下一个策略测试...")
            time.sleep(5)
    
    # 打印所有策略的对比结果
    if len(all_results) > 1:
        print(f"\n{'='*100}")
        print(f"所有策略对比结果")
        print(f"{'='*100}")
        
        for result in all_results:
            strategy_name = result['strategy']
            set_success_rate = result['set_results']['success_rate'] if result['set_results'] else 0
            get_success_rate = result['get_results']['success_rate'] if result['get_results'] else 0
            repair_success_rate = result['repair_results']['success_rate'] if result['repair_results'] else 0
            avg_repair_time = result['repair_results']['average_repair_time'] if result['repair_results'] else 0
            
            print(f"\n📊 {strategy_name}:")
            print(f"  SET成功率: {set_success_rate:.2%}")
            print(f"  GET成功率: {get_success_rate:.2%}")
            print(f"  REPAIR成功率: {repair_success_rate:.2%}")
            print(f"  平均修复时间: {avg_repair_time:.2f}秒")
            print(f"  总耗时: {result['total_duration']:.2f}秒")
        
        print(f"{'='*100}")
    
    print("\n🎉 所有测试完成!")

if __name__ == "__main__":
    main()