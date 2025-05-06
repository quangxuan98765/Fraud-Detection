function createNetworkGraph(data) {
    // Xóa biểu đồ cũ nếu có
    d3.select("#network-graph").html("");
    
    const width = document.getElementById('network-graph').clientWidth;
    const height = document.getElementById('network-graph').clientHeight;
    
    // Kiểm tra dữ liệu
    if (!data.network || !data.network.nodes || data.network.nodes.length === 0) {
        d3.select("#network-graph")
            .append("div")
            .attr("class", "d-flex justify-content-center align-items-center h-100")
            .append("div")
            .attr("class", "text-center")
            .html(`
                <i class="fas fa-exclamation-circle fa-3x text-warning mb-3"></i>
                <p class="lead">Không có dữ liệu phân tích gian lận.<br>Hãy chạy phân tích trước!</p>
            `);
        return;
    }
    
    // Tạo SVG container
    const svg = d3.select("#network-graph")
        .append("svg")
        .attr("width", width)
        .attr("height", height)
        .call(d3.zoom().on("zoom", function(event) {
            container.attr("transform", event.transform);
        }));
        
    const container = svg.append("g");
    
    // Color scale dựa trên fraud score
    const colorScale = d3.scaleLinear()
        .domain([0, 0.5, 0.7, 1.0])
        .range(["#4CAF50", "#FFC107", "#FF9800", "#F44336"]);
    
    // Size scale dựa trên fraud score
    const sizeScale = d3.scaleLinear()
        .domain([0, 1])
        .range([5, 15]);
    
    // Định nghĩa force simulation
    const simulation = d3.forceSimulation(data.network.nodes)
        .force("link", d3.forceLink(data.network.links).id(d => d.id).distance(80))
        .force("charge", d3.forceManyBody().strength(-300))
        .force("center", d3.forceCenter(width / 2, height / 2))
        .force("x", d3.forceX(width / 2).strength(0.05))
        .force("y", d3.forceY(height / 2).strength(0.05))
        .force("collision", d3.forceCollide().radius(d => sizeScale(d.score || 0) + 5));
    
    // Thêm links (edges)
    const link = container.append("g")
        .selectAll("line")
        .data(data.network.links)
        .enter()
        .append("line")
        .attr("class", "link")
        .attr("stroke-width", 1.5)
        .attr("stroke", "#999");
    
    // Thêm nodes
    const node = container.append("g")
        .selectAll("circle")
        .data(data.network.nodes)
        .enter()
        .append("circle")
        .attr("class", "node")
        .attr("r", d => sizeScale(d.score || 0))
        .attr("fill", d => colorScale(d.score || 0))
        .attr("stroke", "#fff")
        .attr("stroke-width", 1.5)
        .call(d3.drag()
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended));
    
    // Thêm tooltips
    node.append("title")
        .text(d => `${d.id}\nĐiểm gian lận: ${d.score ? d.score.toFixed(2) : 'N/A'}`);
    
    // Thêm nhãn cho các node có điểm cao
    container.append("g")
        .selectAll("text")
        .data(data.network.nodes.filter(d => d.score > 0.7))
        .enter()
        .append("text")
        .attr("dx", 15)
        .attr("dy", ".35em")
        .text(d => d.id)
        .style("font-size", "10px")
        .style("fill", "#000");
    
    // Legend
    const legend = svg.append("g")
        .attr("class", "legend")
        .attr("transform", "translate(20,20)");
    
    legend.append("text")
        .attr("x", 0)
        .attr("y", 0)
        .text("Điểm gian lận:")
        .style("font-weight", "bold")
        .style("font-size", "12px");
    
    const legendItems = [
        {color: "#4CAF50", text: "Thấp (< 0.5)"},
        {color: "#FFC107", text: "Trung bình (0.5-0.7)"},
        {color: "#FF9800", text: "Cao (0.7-0.9)"},
        {color: "#F44336", text: "Rất cao (> 0.9)"}
    ];
    
    legendItems.forEach((item, i) => {
        const legendRow = legend.append("g")
            .attr("transform", `translate(0, ${i * 20 + 20})`);
        
        legendRow.append("circle")
            .attr("cx", 5)
            .attr("cy", 0)
            .attr("r", 5)
            .attr("fill", item.color);
        
        legendRow.append("text")
            .attr("x", 15)
            .attr("y", 4)
            .text(item.text)
            .style("font-size", "11px");
    });
    
    // Cập nhật vị trí trong mỗi tick của simulation
    simulation.on("tick", () => {
        link
            .attr("x1", d => d.source.x)
            .attr("y1", d => d.source.y)
            .attr("x2", d => d.target.x)
            .attr("y2", d => d.target.y);
        
        node
            .attr("cx", d => d.x)
            .attr("cy", d => d.y);
        
        container.selectAll("text")
            .attr("x", d => d.x)
            .attr("y", d => d.y);
    });
    
    // Hàm xử lý kéo thả node
    function dragstarted(event, d) {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }
    
    function dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
    }
    
    function dragended(event, d) {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
    }
    
    // Zoom controls
    document.getElementById('zoomIn').addEventListener('click', function() {
        svg.transition().call(
            d3.zoom().on("zoom", function(event) {
                container.attr("transform", event.transform);
            }).scaleBy, 1.5
        );
    });
    
    document.getElementById('zoomOut').addEventListener('click', function() {
        svg.transition().call(
            d3.zoom().on("zoom", function(event) {
                container.attr("transform", event.transform);
            }).scaleBy, 0.75
        );
    });
    
    document.getElementById('resetZoom').addEventListener('click', function() {
        svg.transition().call(
            d3.zoom().on("zoom", function(event) {
                container.attr("transform", event.transform);
            }).transform, d3.zoomIdentity
        );
    });
}