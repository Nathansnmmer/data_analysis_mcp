import os
import sqlite3
import pandas as pd
import json
import tempfile
import uuid
from pathlib import Path
from typing import Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.prompts import base
import openpyxl  # for Excel files
import chardet  # for encoding detection

mcp = FastMCP("Data Analysis Toolkit", dependencies=["pandas", "openpyxl", "chardet"])

# 全局数据库连接管理
_db_connections = {}

def get_or_create_db(db_name: str = "default") -> sqlite3.Connection:
    """
    获取或创建SQLite数据库连接
    """
    if db_name not in _db_connections:
        # 创建临时数据库文件
        temp_dir = tempfile.gettempdir()
        db_path = os.path.join(temp_dir, f"data_analysis_{db_name}_{uuid.uuid4().hex[:8]}.db")
        conn = sqlite3.connect(db_path, check_same_thread=False)
        _db_connections[db_name] = {
            'connection': conn,
            'path': db_path,
            'tables': set()
        }
    return _db_connections[db_name]['connection']

def detect_file_encoding(file_path: str) -> str:
    """
    检测文件编码
    """
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)  # 读取前10KB检测编码
            result = chardet.detect(raw_data)
            return result['encoding'] or 'utf-8'
    except:
        return 'utf-8'

def load_file_to_dataframe(file_path: str) -> pd.DataFrame:
    """
    根据文件类型加载数据到DataFrame
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    file_ext = file_path.suffix.lower()
    
    if file_ext == '.csv':
        # 检测编码
        encoding = detect_file_encoding(str(file_path))
        try:
            df = pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            # 如果检测的编码失败，尝试常见编码
            for enc in ['utf-8', 'gbk', 'gb2312', 'latin-1']:
                try:
                    df = pd.read_csv(file_path, encoding=enc)
                    break
                except:
                    continue
            else:
                raise ValueError("无法确定文件编码")
    
    elif file_ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path)
    
    elif file_ext == '.json':
        df = pd.read_json(file_path)
    
    elif file_ext == '.parquet':
        df = pd.read_parquet(file_path)
    
    elif file_ext == '.tsv':
        encoding = detect_file_encoding(str(file_path))
        df = pd.read_csv(file_path, sep='\t', encoding=encoding)
    
    else:
        raise ValueError(f"不支持的文件格式: {file_ext}")
    
    return df

@mcp.tool()
def import_file(file_path: str, table_name: Optional[str] = None, db_name: str = "default", 
                sheet_name: Optional[str] = None, encoding: Optional[str] = None):
    """
    导入文件到SQLite数据库
    
    Args:
        file_path: 文件路径
        table_name: 表名（可选，默认使用文件名）
        db_name: 数据库名称（可选，默认为"default"）
        sheet_name: Excel表格的sheet名称（可选）
        encoding: 文件编码（可选，自动检测）
    """
    try:
        file_path = Path(file_path)
        
        if not file_path.exists():
            return {
                "status": "error",
                "message": f"文件不存在: {file_path}"
            }
        
        # 如果没有指定表名，使用文件名
        if not table_name:
            table_name = file_path.stem.replace(' ', '_').replace('-', '_')
        
        # 根据文件类型加载数据
        file_ext = file_path.suffix.lower()
        
        if file_ext == '.csv':
            enc = encoding or detect_file_encoding(str(file_path))
            try:
                df = pd.read_csv(file_path, encoding=enc)
            except UnicodeDecodeError:
                for enc in ['utf-8', 'gbk', 'gb2312', 'latin-1']:
                    try:
                        df = pd.read_csv(file_path, encoding=enc)
                        break
                    except:
                        continue
                else:
                    raise ValueError("无法确定文件编码")
        
        elif file_ext in ['.xlsx', '.xls']:
            if sheet_name:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            else:
                df = pd.read_excel(file_path)
        
        elif file_ext == '.json':
            df = pd.read_json(file_path)
        
        elif file_ext == '.parquet':
            df = pd.read_parquet(file_path)
        
        elif file_ext == '.tsv':
            enc = encoding or detect_file_encoding(str(file_path))
            df = pd.read_csv(file_path, sep='\t', encoding=enc)
        
        else:
            return {
                "status": "error",
                "message": f"不支持的文件格式: {file_ext}"
            }
        
        # 获取数据库连接
        conn = get_or_create_db(db_name)
        
        # 导入数据到SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        # 记录表名
        _db_connections[db_name]['tables'].add(table_name)
        
        # 获取基本统计信息
        stats = {
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": df.columns.tolist(),
            "column_types": df.dtypes.astype(str).to_dict()
        }
        
        return {
            "status": "success",
            "message": f"成功导入文件 {file_path.name}",
            "database": db_name,
            "table": table_name,
            "statistics": stats
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"导入文件时出错: {str(e)}"
        }

@mcp.tool()
def list_tables(db_name: str = "default"):
    """
    列出数据库中的所有表
    """
    try:
        if db_name not in _db_connections:
            return {
                "status": "error",
                "message": f"数据库 {db_name} 不存在"
            }
        
        conn = _db_connections[db_name]['connection']
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        return {
            "status": "success",
            "database": db_name,
            "tables": tables
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"列出表时出错: {str(e)}"
        }

@mcp.tool()
def describe_table(table_name: str, db_name: str = "default"):
    """
    描述表结构和基本统计信息
    """
    try:
        if db_name not in _db_connections:
            return {
                "status": "error",
                "message": f"数据库 {db_name} 不存在"
            }
        
        conn = _db_connections[db_name]['connection']
        
        # 获取表结构
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = cursor.fetchall()
        
        if not columns_info:
            return {
                "status": "error",
                "message": f"表 {table_name} 不存在"
            }
        
        # 获取行数
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        
        # 获取前5行数据样本
        df_sample = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 5", conn)
        
        # 获取数值列的统计信息
        numeric_stats = {}
        for col in df_sample.select_dtypes(include=['number']).columns:
            cursor.execute(f"SELECT MIN({col}), MAX({col}), AVG({col}) FROM {table_name};")
            min_val, max_val, avg_val = cursor.fetchone()
            numeric_stats[col] = {
                "min": min_val,
                "max": max_val,
                "avg": round(avg_val, 2) if avg_val else None
            }
        
        return {
            "status": "success",
            "database": db_name,
            "table": table_name,
            "row_count": row_count,
            "columns": [{"name": col[1], "type": col[2]} for col in columns_info],
            "sample_data": df_sample.to_dict('records'),
            "numeric_statistics": numeric_stats
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"描述表时出错: {str(e)}"
        }

@mcp.tool()
def execute_sql(query: str, db_name: str = "default", limit: int = 100):
    """
    执行SQL查询
    
    Args:
        query: SQL查询语句
        db_name: 数据库名称
        limit: 结果限制条数
    """
    try:
        if db_name not in _db_connections:
            return {
                "status": "error",
                "message": f"数据库 {db_name} 不存在"
            }
        
        conn = _db_connections[db_name]['connection']
        
        # 添加LIMIT子句防止返回过多数据
        query = query.strip()
        if not query.upper().startswith(('SELECT', 'WITH')):
            return {
                "status": "error",
                "message": "只支持SELECT和WITH查询语句"
            }
        
        # 如果查询中没有LIMIT，自动添加
        if 'LIMIT' not in query.upper():
            query = f"{query} LIMIT {limit}"
        
        df = pd.read_sql_query(query, conn)
        
        return {
            "status": "success",
            "database": db_name,
            "query": query,
            "row_count": len(df),
            "columns": df.columns.tolist(),
            "data": df.to_dict('records')
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"执行SQL时出错: {str(e)}"
        }

@mcp.tool()
def generate_analysis_report(table_name: str, db_name: str = "default", analysis_type: str = "basic"):
    """
    生成数据分析报告
    
    Args:
        table_name: 表名
        db_name: 数据库名称
        analysis_type: 分析类型 (basic, statistical, correlation)
    """
    try:
        if db_name not in _db_connections:
            return {
                "status": "error",
                "message": f"数据库 {db_name} 不存在"
            }
        
        conn = _db_connections[db_name]['connection']
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        
        report = {
            "table": table_name,
            "analysis_type": analysis_type,
            "basic_info": {
                "rows": len(df),
                "columns": len(df.columns),
                "memory_usage": f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
            }
        }
        
        if analysis_type in ["basic", "statistical", "correlation"]:
            # 基础统计
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                report["numeric_summary"] = df[numeric_cols].describe().to_dict()
            
            # 缺失值统计
            missing_data = df.isnull().sum()
            if missing_data.sum() > 0:
                report["missing_values"] = missing_data[missing_data > 0].to_dict()
            
            # 数据类型统计
            report["data_types"] = df.dtypes.value_counts().to_dict()
        
        if analysis_type in ["statistical", "correlation"]:
            # 重复值统计
            duplicates = df.duplicated().sum()
            if duplicates > 0:
                report["duplicate_rows"] = duplicates
            
            # 唯一值统计
            unique_counts = {}
            for col in df.columns:
                unique_count = df[col].nunique()
                if unique_count < len(df):  # 不是所有值都唯一
                    unique_counts[col] = unique_count
            if unique_counts:
                report["unique_value_counts"] = unique_counts
        
        if analysis_type == "correlation":
            # 相关性分析
            numeric_df = df.select_dtypes(include=['number'])
            if len(numeric_df.columns) > 1:
                correlation_matrix = numeric_df.corr()
                # 找出高相关性的列对
                high_corr_pairs = []
                for i in range(len(correlation_matrix.columns)):
                    for j in range(i+1, len(correlation_matrix.columns)):
                        corr_val = correlation_matrix.iloc[i, j]
                        if abs(corr_val) > 0.7:  # 高相关性阈值
                            high_corr_pairs.append({
                                "column1": correlation_matrix.columns[i],
                                "column2": correlation_matrix.columns[j],
                                "correlation": round(corr_val, 3)
                            })
                if high_corr_pairs:
                    report["high_correlations"] = high_corr_pairs
        
        return {
            "status": "success",
            "database": db_name,
            "report": report
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"生成分析报告时出错: {str(e)}"
        }

@mcp.tool()
def export_query_result(query: str, output_path: str, db_name: str = "default", format: str = "csv"):
    """
    导出查询结果到文件
    
    Args:
        query: SQL查询语句
        output_path: 输出文件路径
        db_name: 数据库名称
        format: 输出格式 (csv, excel, json)
    """
    try:
        if db_name not in _db_connections:
            return {
                "status": "error",
                "message": f"数据库 {db_name} 不存在"
            }
        
        conn = _db_connections[db_name]['connection']
        df = pd.read_sql_query(query, conn)
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if format.lower() == "csv":
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        elif format.lower() == "excel":
            df.to_excel(output_path, index=False)
        elif format.lower() == "json":
            df.to_json(output_path, orient='records', indent=2)
        else:
            return {
                "status": "error",
                "message": f"不支持的输出格式: {format}"
            }
        
        return {
            "status": "success",
            "message": f"成功导出 {len(df)} 行数据到 {output_path}",
            "output_path": str(output_path),
            "format": format,
            "row_count": len(df)
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"导出数据时出错: {str(e)}"
        }

@mcp.tool()
def clean_database(db_name: str = "default"):
    """
    清理数据库（删除所有表）
    """
    try:
        if db_name not in _db_connections:
            return {
                "status": "error",
                "message": f"数据库 {db_name} 不存在"
            }
        
        conn = _db_connections[db_name]['connection']
        cursor = conn.cursor()
        
        # 获取所有表名
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        # 删除所有表
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table};")
        
        conn.commit()
        _db_connections[db_name]['tables'].clear()
        
        return {
            "status": "success",
            "message": f"成功清理数据库 {db_name}，删除了 {len(tables)} 个表",
            "deleted_tables": tables
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"清理数据库时出错: {str(e)}"
        }

@mcp.prompt()
def analyze_data_file(file_path: str, analysis_goals: str = "基础分析", ctx=None):
    """
    分析数据文件并生成完整的分析报告
    
    Args:
        file_path: 数据文件路径
        analysis_goals: 分析目标描述
    """
    try:
        # 导入文件
        import_result = import_file(file_path)
        
        if import_result["status"] == "error":
            return [
                base.UserMessage(f"分析文件 {file_path}"),
                base.AssistantMessage(f"文件导入失败: {import_result['message']}")
            ]
        
        table_name = import_result["table"]
        stats = import_result["statistics"]
        
        # 生成分析报告
        report_result = generate_analysis_report(table_name, analysis_type="correlation")
        
        if report_result["status"] == "error":
            analysis_text = "无法生成详细分析报告"
        else:
            report = report_result["report"]
            
            # 构建分析报告文本
            analysis_text = f"# 数据文件分析报告\n\n"
            analysis_text += f"## 文件信息\n"
            analysis_text += f"- 文件路径: {file_path}\n"
            analysis_text += f"- 导入表名: {table_name}\n"
            analysis_text += f"- 数据行数: {stats['rows']}\n"
            analysis_text += f"- 数据列数: {stats['columns']}\n\n"
            
            analysis_text += f"## 列信息\n"
            for i, (col, dtype) in enumerate(stats['column_types'].items(), 1):
                analysis_text += f"{i}. **{col}** - {dtype}\n"
            analysis_text += "\n"
            
            if "numeric_summary" in report:
                analysis_text += f"## 数值列统计摘要\n"
                for col, summary in report["numeric_summary"].items():
                    analysis_text += f"### {col}\n"
                    analysis_text += f"- 均值: {summary.get('mean', 'N/A'):.2f}\n" if isinstance(summary.get('mean'), (int, float)) else f"- 均值: N/A\n"
                    analysis_text += f"- 标准差: {summary.get('std', 'N/A'):.2f}\n" if isinstance(summary.get('std'), (int, float)) else f"- 标准差: N/A\n"
                    analysis_text += f"- 最小值: {summary.get('min', 'N/A')}\n"
                    analysis_text += f"- 最大值: {summary.get('max', 'N/A')}\n\n"
            
            if "missing_values" in report:
                analysis_text += f"## 缺失值分析\n"
                for col, missing_count in report["missing_values"].items():
                    missing_pct = (missing_count / stats['rows']) * 100
                    analysis_text += f"- **{col}**: {missing_count} 个缺失值 ({missing_pct:.1f}%)\n"
                analysis_text += "\n"
            
            if "high_correlations" in report:
                analysis_text += f"## 高相关性分析\n"
                for corr in report["high_correlations"]:
                    analysis_text += f"- **{corr['column1']}** 与 **{corr['column2']}**: 相关系数 {corr['correlation']}\n"
                analysis_text += "\n"
            
            analysis_text += f"## 分析建议\n"
            analysis_text += f"根据数据特征，建议进行以下分析:\n"
            analysis_text += f"1. 使用 `execute_sql` 工具进行具体的数据查询\n"
            analysis_text += f"2. 针对数值列进行分布分析和异常值检测\n"
            analysis_text += f"3. 分析分类变量的频次分布\n"
            analysis_text += f"4. 根据业务需求进行分组统计和趋势分析\n\n"
            
            analysis_text += f"## 示例查询\n"
            analysis_text += f"```sql\n"
            analysis_text += f"-- 查看前10行数据\n"
            analysis_text += f"SELECT * FROM {table_name} LIMIT 10;\n\n"
            analysis_text += f"-- 统计各列的非空值数量\n"
            analysis_text += f"SELECT COUNT(*) as total_rows FROM {table_name};\n"
            analysis_text += f"```\n"
        
        return [
            base.UserMessage(f"请分析数据文件: {file_path}，分析目标: {analysis_goals}"),
            base.AssistantMessage(analysis_text),
            base.AssistantMessage("数据已成功导入并分析完成！您可以使用 `execute_sql` 工具进行更详细的数据查询和分析。有什么具体的分析需求吗？")
        ]
        
    except Exception as e:
        return [
            base.UserMessage(f"分析文件 {file_path} 时出错"),
            base.AssistantMessage(f"分析文件时发生错误: {str(e)}\n\n请检查文件路径是否正确，以及文件格式是否支持。")
        ]

if __name__ == "__main__":
    mcp.run()