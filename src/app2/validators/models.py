class ValidationResult:
    def __init__(self, status: str, errors: list[str], warnings: list[str], infos: list[str], duration_ms: int):
        self.status = status
        self.errors = errors
        self.warnings = warnings
        self.infos = infos
        self.duration_ms = duration_ms
