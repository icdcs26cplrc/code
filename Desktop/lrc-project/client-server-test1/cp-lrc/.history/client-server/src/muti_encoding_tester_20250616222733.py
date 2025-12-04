#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多编码条带配置策略测试程序
基于提供的编码策略表格进行全面测试
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

class EncodingStrategy:
    """编码策略类"""
    def __init__(self, name: str, k: int, r: int, l: int, block_size: int = 4096):
        self.name = name  # 编码方案名称
        self.k = k        # 数据块数量
        self.r = r        # 全局校验块数量  
        self.l = l        # 本地校验块数量
        self.block_size = block_size
        self.total_blocks = k + r + l
    
    def get_coordinator_config(self) -> str:
        """获取coordinator配置字符串"""
        return f"{self.name} {self.k} {self.l} {self.r} {self.block_size}"
    
    def get_datanode_config(self) -> str:
        """获取datanode配置字符串"""
        return f"{self.k} {self.r} {self.l} {self.block_size}"
    
    def __str__(self):
        return f"{self.name}({self.k},{self.k+self.r+self.l},{self.r},{self.l})"

class MultiEncodingTester:
    """多编码策略测试器"""
    
    def __init__(self, target_dir="../target", testfile_dir="../testfile"):
        self.target_dir = Path(target_dir)
        self.testfile_dir = Path(testfile_dir)
        self.processes = []
        self.running = True
        self.current_strategy = None
        
        # 确保目录存在
        self.testfile_dir.mkdir(exist_ok=True, parents=True)
        
        # 从表格数据创建编码策略
        self.encoding_strategies = self._create_encoding_strategies()
        
        # 测试结果记录
        self.test_results = []
        
        # 注册信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def _create_encoding_strategies(self) -> List[EncodingStrategy]:
        """根据表格创建编码策略列表"""
        strategies = []
        
        # 从表格数据创建策略配置
        table_configs = [
            # (n, k, r, l) 格式
            (10, 6, 2, 2),  # 示例2  
            (12, 6, 3, 3),  # 示例3
            (16, 10, 4, 2), # 示例4
            (16, 12, 2, 2), # 示例5
            (17, 12, 3, 2), # 示例6
            (18, 12, 2, 4), # 示例7
            (20, 12, 4, 4), # 示例8
            (26, 20, 1, 5), # 示例9
            (28, 20, 3, 5), # 示例10
            (28, 24, 2, 2), # 示例11
            (33, 24, 7, 2), # 示例12
            (55, 48, 3, 4), # 示例13
            (55, 48, 4, 3), # 示例14
            (67, 48, 17, 2), # 示例15
            (80, 72, 4, 4), # 示例16
            (95, 72, 19, 4), # 示例17
            (105, 96, 5, 4), # 示例18
        ]
        
        # 编码方案名称列表
        encoding_names = ["azure_lrc", "azure_lrc_1", "optimal", "uniform", "sep-uni","sep-azure"]
        
        for i, (n, k, r, l) in enumerate(table_configs):
            # 为每个配置创建多种编码方案
            for encoding_name in encoding_names:
                strategy = EncodingStrategy(
                    name=encoding_name,
                    k=k, r=r, l=l,
                    block_size=1048576  # 默认块大小为1MB
                )
                strategy.config_id = i + 1  # 配置ID
                strategies.append(strategy)
        
        return strategies

    def signal_handler(self, signum, frame):
        """处理中断信号"""
        print("\n正在安全关闭所有进程...")
        self.running = False
        self.cleanup()
        sys.exit(0)

    def generate_test_data(self, size: int = None) -> str:
        """生成测试数据"""
        if size is None:
            size = random.randint(102400, 409600)  # 100KB-400KB
        
        # 生成可读的测试数据
        data = []
        words = ["test", "data", "encoding", "strategy", "distributed", "storage", "lrc", "repair"]
        
        current_size = 0
        while current_size < size:
            word = random.choice(words)
            data.append(word)
            current_size += len(word) + 1
        
        result = " ".join(data)
        if len(result) < size:
            result += "x" * (size - len(result))
        
        return result[:size]

    def create_test_file(self, filename: str, data: str) -> str:
        """创建测试文件"""
        filepath = self.testfile_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(data)
        return str(filepath)
    

    def start_coordinator(self, strategy: EncodingStrategy):
        """启动coordinator"""
        print(f"启动 Coordinator with strategy: {strategy}")
        
        def run_coordinator():
            try:
                proc = subprocess.Popen(
                    [str(self.target_dir / "coordinator")],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=str(self.target_dir)
                )
                self.processes.append(proc)
                
                # 发送配置
                config = strategy.get_coordinator_config()
                proc.stdin.write(config + "\n")
                proc.stdin.flush()
                
                # 读取输出
                while self.running and proc.poll() is None:
                    line = proc.stdout.readline()
                    if line:
                        print(f"[Coordinator] {line.strip()}")
                        
            except Exception as e:
                print(f"启动Coordinator失败: {e}")
        
        thread = threading.Thread(target=run_coordinator, daemon=True)
        thread.start()
        time.sleep(3)

    def start_proxy(self):
        """启动proxy"""
        def run_proxy():
            try:
                proc = subprocess.Popen(
                    [str(self.target_dir / "proxy")],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=str(self.target_dir)
                )
                self.processes.append(proc)
                
                while self.running and proc.poll() is None:
                    line = proc.stdout.readline()
                    if line:
                        print(f"[Proxy] {line.strip()}")
                        
            except Exception as e:
                print(f"启动Proxy失败: {e}")
        
        thread = threading.Thread(target=run_proxy, daemon=True)
        thread.start()
        time.sleep(2)

    def start_datanode(self, strategy: EncodingStrategy):
        """启动datanode"""
        def run_datanode():
            try:
                proc = subprocess.Popen(
                    [str(self.target_dir / "datanode")],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=str(self.target_dir)
                )
                self.processes.append(proc)
                
                # 发送配置
                config = strategy.get_datanode_config()
                proc.stdin.write(config + "\n")
                proc.stdin.flush()
                
                while self.running and proc.poll() is None:
                    line = proc.stdout.readline()
                    if line:
                        print(f"[Datanode] {line.strip()}")
                        
            except Exception as e:
                print(f"启动Datanode失败: {e}")
        
        thread = threading.Thread(target=run_datanode, daemon=True)
        thread.start()
        time.sleep(2)

    def execute_client_command(self, command: str) -> Tuple[bool, str]:
        # """执行客户端命令"""
        # try:
        #     proc = subprocess.Popen(
        #         [str(self.target_dir / "client")],
        #         stdin=subprocess.PIPE,
        #         stdout=subprocess.PIPE,
        #         stderr=subprocess.PIPE,
        #         text=True,
        #         cwd=str(self.target_dir)
        #     )
            
        #     stdout, stderr = proc.communicate(input=command + "\n", timeout=30)
        #     success = proc.returncode == 0
            
        #     return success, stdout + stderr
            
        # except subprocess.TimeoutExpired:
        #     proc.kill()
        #     return False, "Command timeout"
        # except Exception as e:
        #     return False, str(e)
        """执行客户端命令并记录时延"""
        try:
            start_time = time.time()
            proc = subprocess.Popen(
                [str(self.target_dir / "client")],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, cwd=str(self.target_dir)
            )
            stdout, stderr = proc.communicate(input=command + "\n", timeout=30)
            latency = (time.time() - start_time) * 1000  # 转换为毫秒
            success = proc.returncode == 0
            return success, stdout + stderr, latency
        except subprocess.TimeoutExpired:
            proc.kill()
            return False, "Command timeout", 0.0
        except Exception as e:
            return False, str(e), 0.0

    
    def kill_datanode(self):
        """随机kill datanode进程"""
        if self.datanode_process and self.datanode_process.poll() is None:
            print("正在随机kill datanode...")
            self.datanode_process.terminate()
            try:
                self.datanode_process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.datanode_process.kill()
            print("Datanode已终止")


    def test_single_strategy(self, strategy: EncodingStrategy, num_operations: int = 3) -> Dict:
        """测试单个编码策略"""
        print(f"\n{'='*60}")
        print(f"测试编码策略: {strategy}")
        print(f"配置: k={strategy.k}, r={strategy.r}, l={strategy.l}")
        print(f"{'='*60}")
        
        test_result = {
            'strategy': str(strategy),
            'config': f"k={strategy.k}, r={strategy.r}, l={strategy.l}",
            'trace': [],
            'normal_read_latencies': [],
            'degraded_read_latencies': [],
            'repair_times': [],
            'start_time': time.time()
        }
        
        # 启动系统组件
        self.cleanup()  # 清理之前的进程
        time.sleep(2)
        
        self.current_strategy = strategy
        self.start_coordinator(strategy)
        self.start_proxy() 
        self.start_datanode(strategy)
        
        print("等待系统初始化...")
        time.sleep(5)
        
        # 执行测试操作
        # for i in range(num_operations):
        #     print(f"\n--- 操作 {i+1}/{num_operations} ---")
            
        #     # SET操作
        #     data = self.generate_test_data()
        #     filename = f"test_{strategy.name}_config{strategy.config_id}_{i}.txt"
        #     filepath = self.create_test_file(filename, data)
            
        #     set_cmd = f"set {filepath}"
        #     success, output = self.execute_client_command(set_cmd)
            
        #     operation_result = {
        #         'type': 'SET',
        #         'command': set_cmd,
        #         'success': success,
        #         'data_size': len(data),
        #         'output': output[:200]  # 限制输出长度
        #     }
            
        #     test_result['operations'].append(operation_result)
        #     test_result['total_operations'] += 1
        #     if success:
        #         test_result['success_count'] += 1
        #         print(f"✓ SET操作成功 - 文件: {filename}, 大小: {len(data)} bytes")
        #     else:
        #         print(f"✗ SET操作失败 - {output[:100]}")
            
        #     time.sleep(2)
            
        #     # GET操作
        #     get_cmd = f"get {filename}"
        #     success, output = self.execute_client_command(get_cmd)
            
        #     operation_result = {
        #         'type': 'GET',
        #         'command': get_cmd,
        #         'success': success,
        #         'output': output[:200]
        #     }
            
        #     test_result['operations'].append(operation_result)
        #     test_result['total_operations'] += 1
        #     if success:
        #         test_result['success_count'] += 1
        #         print(f"✓ GET操作成功")
        #     else:
        #         print(f"✗ GET操作失败 - {output[:100]}")
            
        #     time.sleep(2)
        
        # # REPAIR操作
        # print(f"\n--- 修复操作测试 ---")
        # repair_commands = [
        #     "repair stripe_0_block_0 stripe_0_block_6",
        #     "repair stripe_0_block_1 stripe_0_block_5"
        # ]
        
        # for repair_cmd in repair_commands:
        #     success, output = self.execute_client_command(repair_cmd)
            
        #     operation_result = {
        #         'type': 'REPAIR',
        #         'command': repair_cmd,
        #         'success': success,
        #         'output': output[:200]
        #     }
            
        #     test_result['operations'].append(operation_result)
        #     test_result['total_operations'] += 1
        #     if success:
        #         test_result['success_count'] += 1
        #         print(f"✓ REPAIR操作成功 - {repair_cmd}")
        #     else:
        #         print(f"✗ REPAIR操作失败 - {output[:100]}")
            
        #     time.sleep(3)
        
        # # 计算测试结果
        # test_result['end_time'] = time.time()
        # test_result['duration'] = test_result['end_time'] - test_result['start_time']
        # test_result['success_rate'] = test_result['success_count'] / test_result['total_operations'] if test_result['total_operations'] > 0 else 0
        
        # print(f"\n策略 {strategy} 测试完成:")
        # print(f"  成功操作: {test_result['success_count']}/{test_result['total_operations']}")
        # print(f"  成功率: {test_result['success_rate']:.2%}")
        # print(f"  耗时: {test_result['duration']:.2f}秒")

        # 生成trace：50% set，50% get
        filenames = []
        total_ops = num_operations
        set_count = total_ops // 2
        get_count = total_ops - set_count
        kill_triggered = False

        # 执行set操作
        for i in range(set_count):
            data = self.generate_test_data()
            filename = f"test_{strategy.name}_config{strategy.config_id}_{i}.txt"
            filepath = self.create_test_file(filename, data)
            filenames.append(filename)
            
            set_cmd = f"set {filepath}"
            success, output, latency = self.execute_client_command(set_cmd)
            
            op_result = {
                'type': 'SET',
                'command': set_cmd,
                'success': success,
                'latency_ms': latency,
                'data_size': len(data)
            }
            test_result['trace'].append(op_result)
            
            if success:
                print(f"✓ SET {i+1}/{set_count} 成功 - 文件: {filename}, 时延: {latency:.2f}ms")
            else:
                print(f"✗ SET {i+1}/{set_count} 失败 - {output[:100]}")
            time.sleep(1)

        # 执行get操作并随机kill节点
        degraded_read_ratio = 0.3  # 30%的read请求为degraded read
        for i in range(get_count):
            filename = random.choice(filenames)
            get_cmd = f"get {filename}"
            
            # 在30% get操作时kill节点
            if not kill_triggered and i >= int(get_count * (1 - degraded_read_ratio)):
                self.kill_datanode()
                kill_triggered = True
                time.sleep(2)  # 等待系统进入degraded状态
            
            success, output, latency = self.execute_client_command(get_cmd)
            op_result = {
                'type': 'GET',
                'command': get_cmd,
                'success': success,
                'latency_ms': latency,
                'degraded': kill_triggered
            }
            test_result['trace'].append(op_result)
            
            if kill_triggered:
                test_result['degraded_read_latencies'].append(latency)
            else:
                test_result['normal_read_latencies'].append(latency)
            
            if success:
                print(f"✓ GET {i+1}/{get_count} 成功 - {'degraded' if kill_triggered else 'normal'}, 时延: {latency:.2f}ms")
            else:
                print(f"✗ GET {i+1}/{get_count} 失败 - {output[:100]}")
            time.sleep(1)

        # 执行修复操作
        if kill_triggered:
            print("\n--- 执行修复 ---")
            repair_cmd = "repair stripe_0_block_0 stripe_0_block_6"
            start_time = time.time()
            success, output, _ = self.execute_client_command(repair_cmd)
            repair_time = (time.time() - start_time) * 1000  # 毫秒
            test_result['repair_times'].append(repair_time)
            
            op_result = {
                'type': 'REPAIR',
                'command': repair_cmd,
                'success': success,
                'repair_time_ms': repair_time
            }
            test_result['trace'].append(op_result)
            
            if success:
                print(f"✓ REPAIR 成功 - 时延: {repair_time:.2f}ms")
            else:
                print(f"✗ REPAIR 失败 - {output[:100]}")

        # 计算结果
        test_result['end_time'] = time.time()
        test_result['duration'] = test_result['end_time'] - test_result['start_time']
        test_result['avg_normal_read_latency'] = (
            sum(test_result['normal_read_latencies']) / len(test_result['normal_read_latencies'])
            if test_result['normal_read_latencies'] else 0
        )
        test_result['avg_degraded_read_latency'] = (
            sum(test_result['degraded_read_latencies']) / len(test_result['degraded_read_latencies'])
            if test_result['degraded_read_latencies'] else 0
        )
        test_result['avg_repair_time'] = (
            sum(test_result['repair_times']) / len(test_result['repair_times'])
            if test_result['repair_times'] else 0
        )
        
        print(f"\n策略 {strategy} 测试完成:")
        print(f"  平均正常读时延: {test_result['avg_normal_read_latency']:.2f}ms")
        print(f"  平均降级读时延: {test_result['avg_degraded_read_latency']:.2f}ms")
        print(f"  平均修复时间: {test_result['avg_repair_time']:.2f}ms")
        print(f"  总耗时: {test_result['duration']:.2f}秒")
        
        return test_result

    def run_all_strategies_test(self, operations_per_strategy: int = 2):
        """测试所有编码策略"""
        print("="*80)
        print("开始多编码策略全面测试")
        print(f"策略数量: {len(self.encoding_strategies)}")
        print(f"每个策略操作数: {operations_per_strategy}")
        print("="*80)
        
        total_start_time = time.time()
        
        for i, strategy in enumerate(self.encoding_strategies):
            if not self.running:
                break
                
            print(f"\n进度: {i+1}/{len(self.encoding_strategies)}")
            result = self.test_single_strategy(strategy, operations_per_strategy)
            self.test_results.append(result)
            
            # 每测试几个策略后稍作休息
            if (i + 1) % 5 == 0:
                print("\n暂停5秒进行系统清理...")
                time.sleep(5)
        
        # 生成测试报告
        self.generate_test_report(time.time() - total_start_time)

    def run_selected_strategies_test(self, strategy_indices: List[int], operations_per_strategy: int = 3):
        # """测试选定的编码策略"""
        # if not strategy_indices:
        #     print("错误: 未指定测试策略")
        #     return
        
        # selected_strategies = [self.encoding_strategies[i] for i in strategy_indices if 0 <= i < len(self.encoding_strategies)]
        
        # print("="*80)
        # print("开始选定编码策略测试")
        # print(f"选定策略数量: {len(selected_strategies)}")
        # print("="*80)
        
        # for strategy in selected_strategies:
        #     print(f"策略详情: {strategy}")
        
        # total_start_time = time.time()
        
        # for i, strategy in enumerate(selected_strategies):
        #     if not self.running:
        #         break
                
        #     print(f"\n进度: {i+1}/{len(selected_strategies)}")
        #     result = self.test_single_strategy(strategy, operations_per_strategy)
        #     self.test_results.append(result)
        
        # self.generate_test_report(time.time() - total_start_time)
        if not strategy_indices:
            print("错误: 未指定测试策略")
            return
        
        selected_strategies = [self.encoding_strategies[i] for i in strategy_indices if 0 <= i < len(self.encoding_strategies)]
        
        print("="*80)
        print("开始选定编码策略测试")
        print(f"选定策略数量: {len(selected_strategies)}")
        print("="*80)
        
        total_start_time = time.time()
        for i, strategy in enumerate(selected_strategies):
            if not self.running:
                break
            print(f"\n进度: {i+1}/{len(selected_strategies)}")
            result = self.test_single_strategy(strategy, operations_per_strategy)
            self.test_results.append(result)
        
        self.generate_test_report(time.time() - total_start_time)

    def generate_test_report(self, total_duration: float):
        # """生成测试报告"""
        # print("\n" + "="*80)
        # print("测试报告")
        # print("="*80)
        
        # if not self.test_results:
        #     print("无测试结果")
        #     return
        
        # # 总体统计
        # total_strategies = len(self.test_results)
        # total_operations = sum(r['total_operations'] for r in self.test_results)
        # total_success = sum(r['success_count'] for r in self.test_results)
        # overall_success_rate = total_success / total_operations if total_operations > 0 else 0
        
        # print(f"总体统计:")
        # print(f"  测试策略数: {total_strategies}")
        # print(f"  总操作数: {total_operations}")
        # print(f"  成功操作数: {total_success}")
        # print(f"  总体成功率: {overall_success_rate:.2%}")
        # print(f"  总耗时: {total_duration:.2f}秒")
        
        # # 各策略详细结果
        # print(f"\n各策略详细结果:")
        # print("-" * 80)
        # print(f"{'策略':<20} {'配置':<15} {'成功率':<8} {'耗时(s)':<8} {'操作数':<6}")
        # print("-" * 80)
        
        # for result in self.test_results:
        #     strategy_name = result['strategy'].split('(')[0]
        #     config = result['config']
        #     success_rate = f"{result['success_rate']:.1%}"
        #     duration = f"{result['duration']:.1f}"
        #     operations = f"{result['success_count']}/{result['total_operations']}"
            
        #     print(f"{strategy_name:<20} {config:<15} {success_rate:<8} {duration:<8} {operations:<6}")
        
        # # 保存详细报告到文件
        # report_file = f"test_report_{int(time.time())}.json"
        # with open(report_file, 'w', encoding='utf-8') as f:
        #     json.dump({
        #         'summary': {
        #             'total_strategies': total_strategies,
        #             'total_operations': total_operations,
        #             'total_success': total_success,
        #             'overall_success_rate': overall_success_rate,
        #             'total_duration': total_duration
        #         },
        #         'results': self.test_results
        #     }, f, indent=2, ensure_ascii=False)
        print("\n" + "="*80)
        print("测试报告")
        print("="*80)
        
        if not self.test_results:
            print("无测试结果")
            return
        
        total_strategies = len(self.test_results)
        avg_normal_latency = sum(r['avg_normal_read_latency'] for r in self.test_results) / total_strategies
        avg_degraded_latency = sum(r['avg_degraded_read_latency'] for r in self.test_results) / total_strategies
        avg_repair_time = sum(r['avg_repair_time'] for r in self.test_results) / total_strategies
        
        print(f"总体统计:")
        print(f"  测试策略数: {total_strategies}")
        print(f"  平均正常读时延: {avg_normal_latency:.2f}ms")
        print(f"  平均降级读时延: {avg_degraded_latency:.2f}ms")
        print(f"  平均修复时间: {avg_repair_time:.2f}ms")
        print(f"  总耗时: {total_duration:.2f}秒")
        
        report_file = f"test_report_{int(time.time())}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump({
                'summary': {
                    'total_strategies': total_strategies,
                    'avg_normal_read_latency': avg_normal_latency,
                    'avg_degraded_read_latency': avg_degraded_latency,
                    'avg_repair_time': avg_repair_time,
                    'total_duration': total_duration
                },
                'results': self.test_results
            }, f, indent=2, ensure_ascii=False)
        print(f"\n详细报告已保存至: {report_file}")

    def list_available_strategies(self):
        """列出可用的编码策略"""
        print("可用的编码策略:")
        print("-" * 60)
        print(f"{'序号':<4} {'策略名':<15} {'配置':<20} {'总块数':<6}")
        print("-" * 60)
        
        for i, strategy in enumerate(self.encoding_strategies):
            config = f"k={strategy.k},r={strategy.r},l={strategy.l}"
            print(f"{i:<4} {strategy.name:<15} {config:<20} {strategy.total_blocks:<6}")

    def cleanup(self):
        """清理进程"""
        for proc in self.processes:
            if proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except:
                    proc.kill()
        self.processes.clear()

def main():
    # """主函数"""
    # print("多编码条带配置策略测试程序")
    # print("=" * 50)
    
    # tester = MultiEncodingTester()
    
    # try:
    #     if len(sys.argv) == 1:
    #         # 交互模式
    #         while tester.running:
    #             print("\n可用命令:")
    #             print("1. list - 列出所有可用策略")
    #             print("2. test_all [ops] - 测试所有策略")
    #             print("3. test_select [indices] [ops] - 测试选定策略")
    #             print("4. single [index] [ops] - 测试单个策略")
    #             print("5. quit - 退出")
                
    #             cmd = input("\n请输入命令: ").strip().split()
    #             if not cmd:
    #                 continue
                
    #             if cmd[0] == "list":
    #                 tester.list_available_strategies()
                    
    #             elif cmd[0] == "test_all":
    #                 ops = int(cmd[1]) if len(cmd) > 1 else 2
    #                 tester.run_all_strategies_test(ops)
                    
    #             elif cmd[0] == "test_select":
    #                 if len(cmd) < 2:
    #                     print("请指定策略序号，例如: test_select 0,1,2,3")
    #                     continue
    #                 indices = [int(x) for x in cmd[1].split(',')]
    #                 ops = int(cmd[2]) if len(cmd) > 2 else 3
    #                 tester.run_selected_strategies_test(indices, ops)
                    
    #             elif cmd[0] == "single":
    #                 if len(cmd) < 2:
    #                     print("请指定策略序号")
    #                     continue
    #                 index = int(cmd[1])
    #                 ops = int(cmd[2]) if len(cmd) > 2 else 3
    #                 tester.run_selected_strategies_test([index], ops)
                    
    #             elif cmd[0] == "quit":
    #                 break
                    
    #             else:
    #                 print("未知命令")
        
    #     elif sys.argv[1] == "list":
    #         tester.list_available_strategies()
            
    #     elif sys.argv[1] == "test_all":
    #         ops = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    #         tester.run_all_strategies_test(ops)
            
    #     elif sys.argv[1] == "test_select":
    #         if len(sys.argv) < 3:
    #             print("用法: python script.py test_select indices [operations]")
    #             print("例如: python script.py test_select 0,1,2,3 3")
    #             return
    #         indices = [int(x) for x in sys.argv[2].split(',')]
    #         ops = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    #         tester.run_selected_strategies_test(indices, ops)
            
    # except KeyboardInterrupt:
    #     print("\n程序被用户中断")
    # finally:
    #     tester.cleanup()
    print("多编码条带配置策略测试程序")
    print("=" * 50)
    tester = MultiEncodingTester()
    
    try:
        if len(sys.argv) == 1:
            while tester.running:
                print("\n可用命令:")
                print("1. test_select [indices] [ops] - 测试选定策略")
                print("2. quit - 退出")
                cmd = input("\n请输入命令: ").strip().split()
                if not cmd:
                    continue
                if cmd[0] == "test_select":
                    if len(cmd) < 2:
                        print("请指定策略序号，例如: test_select 0,1,2,3")
                        continue
                    indices = [int(x) for x in cmd[1].split(',')]
                    ops = int(cmd[2]) if len(cmd) > 2 else 100
                    tester.run_selected_strategies_test(indices, ops)
                elif cmd[0] == "quit":
                    break
                else:
                    print("未知命令")
        elif sys.argv[1] == "test_select":
            if len(sys.argv) < 3:
                print("用法: python script.py test_select indices [operations]")
                return
            indices = [int(x) for x in sys.argv[2].split(',')]
            ops = int(sys.argv[3]) if len(sys.argv) > 3 else 100
            tester.run_selected_strategies_test(indices, ops)
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    finally:
        tester.cleanup()

if __name__ == "__main__":
    main()
