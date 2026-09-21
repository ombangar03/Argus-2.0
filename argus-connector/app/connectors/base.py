from abc import ABC, abstractmethod 

class BaseConnector(ABC):

    @abstractmethod
    async def fetch(self, request):
        pass