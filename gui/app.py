import threading
import customtkinter as ctk

from robos.robo_pedido_de_compra.pedido_de_compra import run as run_pedido_compra
from robos.robo_chave_nao_existente.chave_nao_existente import run as run_chave_nao_existente
from robos.robo_exclusao_protocolo.exclusao_protocolo import run as run_exclusao_protocolo
from robos.robo_cadastro_prescritor.cadastro_prescritor import run as run_cadastro_prescritor
from robos.robo_produto_reserva.produto_reserva import run as run_produto_reserva


class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Central de Robôs")
        self.geometry("1000x600")

        ctk.set_appearance_mode("dark")
        self.build_interface()

    def build_interface(self):
        self.grid_columnconfigure((0, 1, 2), weight=1)

        robos = [
            {
                "titulo": "Robô: Pedido de Compra",
                "status_var": "status_pc",
                "botao_var": "botao_pedido_compra",
                "label_var": "label_status_pc",
                "callback": self.iniciar_pedido_compra
            },
            {
                "titulo": "Robô: Chave Não Existente",
                "status_var": "status_chave",
                "botao_var": "botao_chave_nao_existente",
                "label_var": "label_status_chave",
                "callback": self.iniciar_chave_nao_existente
            },
            {
                "titulo": "Robô: Protocolo Não Recebimento",
                "status_var": "status_exclusao_protocolo",
                "botao_var": "botao_exclusao_protocolo",
                "label_var": "label_status_exclusao_protocolo",
                "callback": self.iniciar_exclusao_protocolo
            },
            {
                "titulo": "Robô: Cadastro de prescritor",
                "status_var": "status_cadastro_prescritor",
                "botao_var": "botao_cadastro_prescritor",
                "label_var": "label_status_cadastro_prescritor",
                "callback": self.iniciar_cadastro_prescritor
            },
            {
                "titulo": "Robô: Produto em Reserva",
                "status_var": "status_produto_reserva",
                "botao_var": "botao_produto_reserva",
                "label_var": "label_status_produto_reserva",
                "callback": self.iniciar_produto_reserva
            }
        ]

        for index, robo in enumerate(robos):
            col = index % 3
            row = index // 3

            frame = ctk.CTkFrame(self)
            frame.grid(row=row, column=col, padx=10, pady=10, sticky="nsew")

            ctk.CTkLabel(frame, text=robo["titulo"], font=("Arial", 16)).pack(pady=(10, 0))

            status_menu = ctk.CTkOptionMenu(frame, values=["Aguardando atendimento", "Em andamento"])
            status_menu.pack(pady=5)
            setattr(self, robo["status_var"], status_menu)

            botao = ctk.CTkButton(frame, text="Iniciar Robô", command=robo["callback"])
            botao.pack(pady=5)
            setattr(self, robo["botao_var"], botao)

            status_label = ctk.CTkLabel(frame, text="")
            status_label.pack(pady=(0, 10))
            setattr(self, robo["label_var"], status_label)

    def iniciar_pedido_compra(self):
        self.botao_pedido_compra.configure(state="disabled")
        self.label_status_pc.configure(text="Rodando...", compound="left")

        status = self.status_pc.get()
        ativo = "000001" if status == "Aguardando atendimento" else "000006"

        def tarefa():
            try:
                run_pedido_compra(ativo)
            finally:
                self.botao_pedido_compra.configure(state="normal")
                self.label_status_pc.configure(text="✅ Robô finalizado!")

        threading.Thread(target=tarefa).start()

    def iniciar_chave_nao_existente(self):
        self.botao_chave_nao_existente.configure(state="disabled")
        self.label_status_chave.configure(text="Rodando...", compound="left")

        status = self.status_chave.get()
        ativo = "000001" if status == "Aguardando atendimento" else "000006"

        def tarefa():
            try:
                run_chave_nao_existente(ativo)
            finally:
                self.botao_chave_nao_existente.configure(state="normal")
                self.label_status_chave.configure(text="✅ Robô finalizado!")

        threading.Thread(target=tarefa).start()

    def iniciar_exclusao_protocolo(self):
        self.botao_exclusao_protocolo.configure(state="disabled")
        self.label_status_exclusao_protocolo.configure(text="Rodando...", compound="left")

        status = self.status_exclusao_protocolo.get()
        ativo = "000001" if status == "Aguardando atendimento" else "000006"

        def tarefa():
            try:
                run_exclusao_protocolo(ativo)
            finally:
                self.botao_exclusao_protocolo.configure(state="normal")
                self.label_status_exclusao_protocolo.configure(text="✅ Robô finalizado!")

        threading.Thread(target=tarefa).start()

    def iniciar_cadastro_prescritor(self):
        self.botao_cadastro_prescritor.configure(state="disabled")
        self.label_status_cadastro_prescritor.configure(text="Rodando...", compound="left")

        status = self.status_cadastro_prescritor.get()
        ativo = "000001" if status == "Aguardando atendimento" else "000006"

        def tarefa():
            try:
                run_cadastro_prescritor(ativo)
            finally:
                self.botao_cadastro_prescritor.configure(state="normal")
                self.label_status_cadastro_prescritor.configure(text="✅ Robô finalizado!")

        threading.Thread(target=tarefa).start()


    def iniciar_produto_reserva(self):
        self.botao_produto_reserva.configure(state="disabled")
        self.label_status_produto_reserva.configure(text="Rodando...", compound="left")

        status = self.status_produto_reserva.get()
        ativo = "000001" if status == "Aguardando atendimento" else "000006"

        def tarefa():
            try:
                run_produto_reserva(ativo)
            finally:
                self.botao_produto_reserva.configure(state="normal")
                self.label_status_produto_reserva.configure(text="✅ Robô finalizado!")

        threading.Thread(target=tarefa).start()


if __name__ == "__main__":
    app = App()
    app.mainloop()
