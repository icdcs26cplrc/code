#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
改进的多编码条带配置策略测试程序
修复了原有问题并增强了健壮性
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
import re
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from queue import Queue, Empty

class EncodingStrategy:
    """编码策略类"""
    def __init__(self, name: str, k: int, r: int, l: int, block_size: int = 1048576):
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

class ComponentManager:
    """组件管理器 - 管理coordinator, proxy, datanode进程"""
    
    def __init__(self, target_dir: Path):
        self.target_dir = target_dir
        self.processes = {}
        self.output_queues = {}
        self.running = True
        
    def start_component(self, component_name: str, config: Optional[str] = None) -> bool:
        """启动组件"""
        try:
            executable = self.target_dir / component_name
            if not executable.exists():
                print(f"错误: {executable} 不存在")
                return False
                
            proc = subprocess.Popen(
                [str(executable)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=str(self.target_dir),
                bufsize=1,
                universal_newlines=True
            )
            
            self.processes[component_name] = proc
            self.output_queues[component_name] = Queue()
            
            # 启动输出监控线程
            def monitor_output():
                while self.running and proc.poll() is None:
                    try:
                        line = proc.stdout.readline()
                        if line:
                            self.output_queues[component_name].put(line.strip())
                            print(f"[{component_name.upper()}] {line.strip()}")
                    except Exception as e:
                        print(f"[{component_name.upper()}] 输出监控错误: {e}")
                        break
            
            thread = threading.Thread(target=monitor_output, daemon=True)
            thread.start()
            
            # 发送配置
            if config:
                time.sleep(1)  # 等待进程启动
                proc.stdin.write(config + "\n")
                proc.stdin.flush()
                
            return True
            
        except Exception as e:
            print(f"启动{component_name}失败: {e}")
            return False
    
    def get_component_output(self, component_name: str, timeout: float = 1.0) -> List[str]:
        """获取组件输出"""
        if component_name not in self.output_queues:
            return []
            
        outputs = []
        end_time = time.time() + timeout
        
        while time.time() < end_time:
            try:
                line = self.output_queues[component_name].get_nowait()
                outputs.append(line)
            except Empty:
                time.sleep(0.1)
                
        return outputs
    
    def is_component_running(self, component_name: str) -> bool:
        """检查组件是否在运行"""
        if component_name not in self.processes:
            return False
        return self.processes[component_name].poll() is None
    
    def kill_component(self, component_name: str):
        """终止指定组件"""
        if component_name in self.processes:
            proc = self.processes[component_name]
            if proc.poll() is None:
                print(f"正在终止 {component_name}...")
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                print(f"{component_name} 已终止")
    
    def cleanup(self):
        """清理所有进程"""
        self.running = False
        for name, proc in self.processes.items():
            if proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
        self.processes.clear()
        self.output_queues.clear()

class MultiEncodingTester:
    """多编码策略测试器"""
    
    def __init__(self, target_dir="../target", testfile_dir="../testfile"):
        self.target_dir = Path(target_dir)
        self.testfile_dir = Path(testfile_dir)
        self.component_manager = ComponentManager(self.target_dir)
        self.running = True
        self.current_strategy = None
        
        # 确保目录存在
        self.testfile_dir.mkdir(exist_ok=True, parents=True)
        
        # 创建编码策略
        self.encoding_strategies = self._create_encoding_strategies()
        
        # 测试结果记录
        self.test_results = []
        
        # 注册信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def _create_encoding_strategies(self) -> List[EncodingStrategy]:
        """根据表格创建编码策略列表"""
        strategies = []
        
        # 简化的配置用于测试
        table_configs = [
            (10, 6, 2, 2),  # azure_lrc配置
            (12, 6, 3, 3),  # 其他配置...
        ]
        
        # 编码方案名称列表
        encoding_names = ["azure", "azure_1", "optimal", "uniform", "sep-azure","sep-uni"]
        
        for i, (n, k, r, l) in enumerate(table_configs):
            for encoding_name in encoding_names:
                strategy = EncodingStrategy(
                    name=encoding_name,
                    k=k, r=r, l=l,
                    block_size=1048576
                )
                strategy.config_id = i + 1
                strategies.append(strategy)
        
        return strategies

    def signal_handler(self, signum, frame):
        """处理中断信号"""
        print("\n正在安全关闭所有进程...")
        self.running = False
        self.component_manager.cleanup()
        sys.exit(0)

    def generate_test_data(self, size: int = None) -> str:
        """生成测试数据"""
        if size is None:
            size = random.randint(102400, 409600)  # 100KB-400KB
        
        # 生成随机字符串
        chars = string.ascii_letters + string.digits + " "
        return ''.join(random.choice(chars) for _ in range(size))

    def create_test_file(self, filename: str, data: str) -> str:
        """创建测试文件"""
        filepath = self.testfile_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(data)
        return str(filepath)

    def setup_system(self, strategy: EncodingStrategy) -> bool:
        """启动系统组件"""
        print(f"启动系统组件 with strategy: {strategy}")
        
        # 清理之前的进程
        self.component_manager.cleanup()
        time.sleep(2)
        
        self.component_manager = ComponentManager(self.target_dir)
        
        # 启动coordinator
        coordinator_config = strategy.get_coordinator_config()
        if not self.component_manager.start_component("coordinator", coordinator_config):
            return False
        time.sleep(3)
        
        # 启动proxy
        if not self.component_manager.start_component("proxy"):
            return False
        time.sleep(2)
        
        # 启动datanode
        datanode_config = strategy.get_datanode_config()
        if not self.component_manager.start_component("datanode", datanode_config):
            return False
        time.sleep(3)
        
        # 验证所有组件都在运行
        components = ["coordinator", "proxy", "datanode"]
        for comp in components:
            if not self.component_manager.is_component_running(comp):
                print(f"组件 {comp} 启动失败")
                return False
        
        print("所有系统组件启动成功")
        return True

    def execute_client_command(self, command: str, timeout: int = 30) -> Tuple[bool, str, float, Dict]:
        """执行客户端命令并解析结果"""
        try:
            start_time = time.time()
            
            # 获取执行前的proxy输出
            pre_outputs = self.component_manager.get_component_output("proxy", 0.5)
            
            # 执行客户端命令
            proc = subprocess.Popen(
                [str(self.target_dir / "client")],
                stdin=subprocess.PIPE, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT,
                text=True, 
                cwd=str(self.target_dir)
            )
            
            stdout, _ = proc.communicate(input=command + "\n", timeout=timeout)
            latency = (time.time() - start_time) * 1000
            
            # 获取执行后的proxy输出
            time.sleep(1)  # 等待proxy输出
            post_outputs = self.component_manager.get_component_output("proxy", 2.0)
            
            # 解析结果
            result_info = self._parse_command_result(command, stdout, pre_outputs + post_outputs)
            success = result_info.get('success', proc.returncode == 0)
            
            return success, stdout, latency, result_info
            
        except subprocess.TimeoutExpired:
            proc.kill()
            return False, "Command timeout", 0.0, {'success': False, 'reason': 'timeout'}
        except Exception as e:
            return False, str(e), 0.0, {'success': False, 'reason': str(e)}

    def _parse_command_result(self, command: str, client_output: str, proxy_outputs: List[str]) -> Dict:
        """解析命令执行结果"""
        result = {'success': False, 'reason': '', 'details': {}}
        
        cmd_parts = command.strip().split()
        if not cmd_parts:
            return result
            
        cmd_type = cmd_parts[0].lower()
        
        if cmd_type == 'set':
            # 解析SET命令结果
            # 检查客户端输出
            if "Data sent successfully to proxy" in client_output:
                result['details']['client_success'] = True
            else:
                result['reason'] = 'Client failed to send data'
                return result
            
            # 检查proxy输出中的buffer size变化
            buffer_size_changes = []
            for output in proxy_outputs:
                if "after recv buffer_data size" in output:
                    match = re.search(r'after recv buffer_data size : (\d+)', output)
                    if match:
                        buffer_size_changes.append(int(match.group(1)))
            
            if buffer_size_changes:
                result['details']['buffer_size_changes'] = buffer_size_changes
                result['details']['final_buffer_size'] = buffer_size_changes[-1]
                result['success'] = True
            else:
                result['reason'] = 'No buffer size change detected in proxy'
                
        elif cmd_type == 'get':
            # 解析GET命令结果
            if "GET command received" in client_output:
                result['details']['client_received'] = True
                
                # 提取接收的数据大小
                match = re.search(r'GET command received :(\d+)', client_output)
                if match:
                    result['details']['received_size'] = int(match.group(1))
                    result['success'] = True
                else:
                    result['reason'] = 'Failed to parse received data size'
            else:
                result['reason'] = 'GET command not received properly'
                
            # 检查是否是降级读
            degraded_indicators = [
                "Detected", "failed nodes", "degraded read", "repair request"
            ]
            proxy_output_text = ' '.join(proxy_outputs)
            if any(indicator in proxy_output_text for indicator in degraded_indicators):
                result['details']['degraded_read'] = True
                
        elif cmd_type == 'repair':
            # 解析REPAIR命令结果
            proxy_output_text = ' '.join(proxy_outputs)
            if "sucessfully repair" in proxy_output_text or "repair success" in proxy_output_text:
                result['success'] = True
            else:
                result['reason'] = 'Repair operation not confirmed'
        
        return result

    def simulate_node_failure(self) -> bool:
        """模拟节点故障 - 杀死datanode进程"""
        try:
            # 获取datanode进程
            if not self.component_manager.is_component_running("datanode"):
                print("Datanode 不在运行中")
                return False
            
            # 随机杀死一个datanode子进程
            datanode_proc = self.component_manager.processes["datanode"]
            
            # 发送SIGKILL给一个随机的datanode子进程
            # 这里我们通过向主进程发送特定信号来模拟节点故障
            print("模拟节点故障...")
            
            # 直接终止一部分datanode进程来模拟故障
            self.component_manager.kill_component("datanode")
            time.sleep(2)
            
            # 重新启动datanode (模拟部分节点故障后的恢复)
            if self.current_strategy:
                datanode_config = self.current_strategy.get_datanode_config()
                self.component_manager.start_component("datanode", datanode_config)
                time.sleep(3)
            
            return True
            
        except Exception as e:
            print(f"模拟节点故障失败: {e}")
            return False

    def test_single_strategy(self, strategy: EncodingStrategy, num_operations: int = 50) -> Dict:
        """测试单个编码策略"""
        print(f"\n{'='*60}")
        print(f"测试编码策略: {strategy}")
        print(f"配置: k={strategy.k}, r={strategy.r}, l={strategy.l}")
        print(f"{'='*60}")
        
        test_result = {
            'strategy': str(strategy),
            'config': f"k={strategy.k}, r={strategy.r}, l={strategy.l}",
            'operations': [],
            'statistics': {
                'total_ops': 0,
                'successful_ops': 0,
                'failed_ops': 0,
                'set_ops': 0,
                'get_ops': 0,
                'repair_ops': 0,
                'normal_reads': 0,
                'degraded_reads': 0,
                'avg_latency': 0.0,
                'avg_set_latency': 0.0,
                'avg_get_latency': 0.0,
                'avg_repair_latency': 0.0
            },
            'start_time': time.time()
        }
        
        self.current_strategy = strategy
        
        # 启动系统
        if not self.setup_system(strategy):
            test_result['error'] = "Failed to setup system"
            return test_result
        
        print("等待系统稳定...")
        time.sleep(5)
        
        # 存储的文件名列表
        stored_files = []
        latencies = []
        
        # 执行测试操作
        for i in range(num_operations):
            if not self.running:
                break
            
            # 根据比例决定操作类型
            if i < num_operations * 0.6:  # 前60%为SET操作
                op_type = 'SET'
            elif i < num_operations * 0.9:  # 60%-90%为GET操作
                op_type = 'GET'
            else:  # 最后10%为REPAIR操作
                op_type = 'REPAIR'
            
            operation_result = self._execute_single_operation(op_type, i, stored_files)
            test_result['operations'].append(operation_result)
            
            # 更新统计信息
            self._update_statistics(test_result['statistics'], operation_result)
            latencies.append(operation_result.get('latency_ms', 0))
            
            # 在特定点模拟节点故障
            if i == int(num_operations * 0.7) and op_type == 'GET':
                print("\n--- 模拟节点故障 ---")
                self.simulate_node_failure()
                time.sleep(2)
            
            time.sleep(0.5)  # 操作间隔
        
        # 计算最终统计
        test_result['end_time'] = time.time()
        test_result['duration'] = test_result['end_time'] - test_result['start_time']
        
        if latencies:
            test_result['statistics']['avg_latency'] = sum(latencies) / len(latencies)
        
        self._print_test_summary(test_result)
        return test_result

    def _execute_single_operation(self, op_type: str, index: int, stored_files: List[str]) -> Dict:
        """执行单个操作"""
        operation = {
            'index': index,
            'type': op_type,
            'timestamp': time.time(),
            'success': False,
            'latency_ms': 0.0,
            'details': {}
        }
        
        try:
            if op_type == 'SET':
                # SET操作
                data = self.generate_test_data()
                filename = f"test_{self.current_strategy.name}_config{self.current_strategy.config_id}_{index}.txt"
                filepath = self.create_test_file(filename, data)
                stored_files.append(filename)
                
                command = f"set {filepath}"
                success, output, latency, details = self.execute_client_command(command)
                
                operation.update({
                    'command': command,
                    'success': success,
                    'latency_ms': latency,
                    'data_size': len(data),
                    'filename': filename,
                    'details': details
                })
                
                status = "✓" if success else "✗"
                print(f"{status} SET {index+1} {'成功' if success else '失败'} - "
                      f"文件: {filename}, 时延: {latency:.2f}ms")
                
            elif op_type == 'GET' and stored_files:
                # GET操作
                filename = random.choice(stored_files)
                command = f"get {filename}"
                success, output, latency, details = self.execute_client_command(command)
                
                is_degraded = details.get('details', {}).get('degraded_read', False)
                
                operation.update({
                    'command': command,
                    'success': success,
                    'latency_ms': latency,
                    'filename': filename,
                    'degraded': is_degraded,
                    'details': details
                })
                
                status = "✓" if success else "✗"
                degraded_str = "(降级)" if is_degraded else ""
                print(f"{status} GET {index+1} {'成功' if success else '失败'} - "
                      f"文件: {filename}, 时延: {latency:.2f}ms {degraded_str}")
                
            elif op_type == 'REPAIR':
                # REPAIR操作
                command = "repair stripe_0_block_0 stripe_0_block_1"
                success, output, latency, details = self.execute_client_command(command, timeout=60)
                
                operation.update({
                    'command': command,
                    'success': success,
                    'latency_ms': latency,
                    'details': details
                })
                
                status = "✓" if success else "✗"
                print(f"{status} REPAIR {index+1} {'成功' if success else '失败'} - "
                      f"时延: {latency:.2f}ms")
        
        except Exception as e:
            operation['error'] = str(e)
            print(f"✗ {op_type} {index+1} 异常 - {str(e)}")
        
        return operation

    def _update_statistics(self, stats: Dict, operation: Dict):
        """更新统计信息"""
        stats['total_ops'] += 1
        
        if operation['success']:
            stats['successful_ops'] += 1
        else:
            stats['failed_ops'] += 1
        
        op_type = operation['type']
        if op_type == 'SET':
            stats['set_ops'] += 1
        elif op_type == 'GET':
            stats['get_ops'] += 1
            if operation.get('degraded', False):
                stats['degraded_reads'] += 1
            else:
                stats['normal_reads'] += 1
        elif op_type == 'REPAIR':
            stats['repair_ops'] += 1

    def _print_test_summary(self, test_result: Dict):
        """打印测试摘要"""
        stats = test_result['statistics']
        print(f"\n策略 {test_result['strategy']} 测试完成:")
        print(f"  总操作数: {stats['total_ops']}")
        print(f"  成功操作: {stats['successful_ops']}")
        print(f"  失败操作: {stats['failed_ops']}")
        print(f"  成功率: {(stats['successful_ops']/stats['total_ops']*100):.1f}%")
        print(f"  平均延迟: {stats['avg_latency']:.2f}ms")
        print(f"  正常读取: {stats['normal_reads']}")
        print(f"  降级读取: {stats['degraded_reads']}")
        print(f"  总耗时: {test_result['duration']:.2f}秒")

    def run_selected_strategies_test(self, strategy_indices: List[int], operations_per_strategy: int = 50):
        """运行选定策略测试"""
        if not strategy_indices:
            print("错误: 未指定测试策略")
            return
        
        selected_strategies = [self.encoding_strategies[i] for i in strategy_indices 
                             if 0 <= i < len(self.encoding_strategies)]
        
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
        """生成测试报告"""
        print("\n" + "="*80)
        print("测试报告")
        print("="*80)
        
        if not self.test_results:
            print("无测试结果")
            return
        
        # 计算总体统计
        total_strategies = len(self.test_results)
        total_ops = sum(r['statistics']['total_ops'] for r in self.test_results)
        total_successful = sum(r['statistics']['successful_ops'] for r in self.test_results)
        avg_success_rate = (total_successful / total_ops * 100) if total_ops > 0 else 0
        
        print(f"总体统计:")
        print(f"  测试策略数: {total_strategies}")
        print(f"  总操作数: {total_ops}")
        print(f"  总成功操作: {total_successful}")
        print(f"  平均成功率: {avg_success_rate:.1f}%")
        print(f"  总测试时间: {total_duration:.2f}秒")
        
        # 保存详细报告
        report_file = f"test_report_{int(time.time())}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump({
                'summary': {
                    'total_strategies': total_strategies,
                    'total_operations': total_ops,
                    'total_successful': total_successful,
                    'avg_success_rate': avg_success_rate,
                    'total_duration': total_duration
                },
                'results': self.test_results
            }, f, indent=2, ensure_ascii=False)
        print(f"\n详细报告已保存至: {report_file}")

    def cleanup(self):
        """清理资源"""
        self.component_manager.cleanup()

def main():
    print("多编码条带配置策略测试程序")
    print("=" * 50)
    tester = MultiEncodingTester()
    
    try:
        while tester.running:
            print("\n可用命令:")
            print("1. test_select [indices] [ops] - 测试选定策略")
            print("2. quit - 退出")
            
            cmd = input("\n请输入命令: ").strip().split()
            if not cmd:
                continue
                
            if cmd[0] == "test_select":
                if len(cmd) < 2:
                    print("请指定策略序号，例如: test_select 0 100")
                    continue
                indices = [int(x) for x in cmd[1].split(',')]
                ops = int(cmd[2]) if len(cmd) > 2 else 100
                tester.run_selected_strategies_test(indices, ops)
                
            elif cmd[0] == "quit":
                break
            else:
                print("未知命令")
                
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    finally:
        tester.cleanup()

if __name__ == "__main__":
    main()