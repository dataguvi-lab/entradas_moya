import subprocess
import os
import time

def start_vpn():
    """
    Inicia a conexão VPN usando o OpenVPN
    """
    try:
        # Comando para iniciar a VPN
        cmd = ["sudo", "openvpn", "--config", "/home/ubuntu/vpn_moya/VPN-UDP4-1200-dataguvi-moya-config.ovpn", "--daemon"]
        
        # Executa o comando
        subprocess.run(cmd, check=True)
        print("VPN iniciada com sucesso")
        
        # Aguarda alguns segundos para a conexão se estabelecer
        time.sleep(5)
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Erro ao iniciar VPN: {e}")
        return False

def stop_vpn():
    """
    Para a conexão VPN matando o processo OpenVPN
    """
    try:
        # Procura por processos OpenVPN com o arquivo de configuração específico
        cmd = ["pgrep", "-f", "openvpn.*VPN-UDP4-1200-dataguvi-moya-config.ovpn"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.stdout:
            # Se encontrou processos, mata cada um deles
            pids = result.stdout.strip().split('\n')
            print("Desconectando VPN com PIDs:")
            print('\n'.join(pids))
            
            for pid in pids:
                kill_cmd = ["sudo", "kill", pid.strip()]
                subprocess.run(kill_cmd, check=True)
                
            print("VPN desconectada com sucesso")
            return True
        else:
            print("VPN não está conectada")
            return False
            
    except subprocess.CalledProcessError as e:
        print(f"Erro ao parar VPN: {e}")
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2 or sys.argv[1] not in ['start', 'stop']:
        print("Uso: python vpn_manager.py [start|stop]")
        sys.exit(1)
        
    if sys.argv[1] == 'start':
        start_vpn()
    else:
        stop_vpn()